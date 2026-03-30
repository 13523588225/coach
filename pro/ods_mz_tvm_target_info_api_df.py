# -*- coding: utf-8 -*-
"""秒针TV监测活动目标信息采集脚本"""
import requests
import json
import time
import urllib3
import argparse
from datetime import datetime
from typing import Dict, List
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_target_info_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaign/target/info",
    "timeout": 30,
    "request_interval": 0.2
}

# ODPS配置
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "target_info": "ods_mz_tvm_target_info_api_df",
    "campaign_list": "ods_mz_tvm_campaigns_list_api_df"
}


def get_etl_datetime() -> str:
    """获取标准时间戳，格式：yyyy-MM-dd HH:mm:ss"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_log() -> str:
    """获取日志打印时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """空值安全转换为字符串，过滤None/空/Null"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


def get_tvm_api_credentials() -> tuple:
    """从数仓表查询TVM接口账号密码
    返回：username, password
    """
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'TVM' limit 1 \
          """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            return record["username"], record["passwords"]
    except errors.ODPSError as e:
        raise Exception(f"查询账号密码失败：{str(e)}")


def get_miaozhen_token() -> str:
    """获取秒针接口访问令牌"""
    try:
        username, password = get_tvm_api_credentials()
        resp = requests.post(
            API_CONFIG["token_url"],
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()

        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误：{token_data.get('error_message')}")

        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token为空")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_ids(dt: str) -> List[str]:
    """从ODPS表查询指定分区的campaign_id
    参数：dt-分区日期
    返回：去重后的campaign_id列表
    """
    try:
        o = ODPS(project=ODPS_PROJECT)
        sql = f"""
        select campaign_id 
        from {TABLE_NAMES['campaign_list']} 
        where dt = '{dt}'
        """
        with o.execute_sql(sql).open_reader() as reader:
            campaign_ids = [to_string(record["campaign_id"]) for record in reader]
            campaign_ids = list(filter(None, set(campaign_ids)))

        print(f"[{get_log()}] 从ODPS查询到{len(campaign_ids)}个有效ID")
        return campaign_ids
    except errors.ODPSError as e:
        raise Exception(f"查询campaign_id失败：{str(e)}")


def get_campaign_target_info(token: str, campaign_id: str) -> List[Dict]:
    """获取单个活动的目标信息
    参数：token-接口令牌, campaign_id-活动ID
    返回：标准化的目标数据列表
    """
    try:
        params = {"access_token": token, "campaign_id": campaign_id}
        full_url = f"{API_CONFIG['campaign_target_info_url']}?access_token={token}&campaign_id={campaign_id}"

        resp = requests.get(
            API_CONFIG["campaign_target_info_url"],
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        target_data = resp.json()

        if target_data.get("error_code") != 0:
            print(f"[{get_log()}] campaign_id={campaign_id} 采集失败")
            return []

        result = target_data.get("result", {})
        if not isinstance(result, dict):
            print(f"[{get_log()}] campaign_id={campaign_id} 返回格式异常")
            return []

        target_list = result.get("target", [])
        etl_time = get_etl_datetime()
        standard_data = []

        for target in target_list:
            if not isinstance(target, dict):
                continue
            standard_data.append({
                "request_campaign_id": to_string(campaign_id),
                "result_campaign_id": to_string(result.get("campaign_id")),
                "target_display": to_string(target.get("target_display")),
                "update_time": to_string(target.get("update_time")),
                "target_name": to_string(target.get("target_name")),
                "target_id": to_string(target.get("target_id")),
                "full_request_url": to_string(full_url),
                "pre_parse_raw_text": to_string(json.dumps(result, ensure_ascii=False)),
                "etl_datetime": to_string(etl_time)
            })

        print(f"[{get_log()}] campaign_id={campaign_id} 采集{len(standard_data)}条数据")
        time.sleep(API_CONFIG["request_interval"])
        return standard_data
    except Exception as e:
        print(f"[{get_log()}] campaign_id={campaign_id} 异常：{str(e)}")
        time.sleep(API_CONFIG["request_interval"])
        return []


def write_to_odps(table_name: str, data: List[List], dt: str):
    """数据写入ODPS表（清空分区+覆盖写入）
    参数：table_name-表名, data-待写入数据, dt-分区日期
    """
    if not data:
        print(f"[{get_log()}] 无数据写入")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表{table_name}不存在")

    table = o.get_table(table_name)
    partition = f"dt='{dt}'"

    if table.exist_partition(partition):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition})")

    with table.open_writer(partition=partition, create_partition=True) as writer:
        writer.write(data)
    print(f"[{get_log()}] 写入成功：{len(data)}条")


def main():
    """脚本主执行流程"""
    try:
        # 字典格式获取dt参数
        dt = args['dt']

        # 核心流程
        token = get_miaozhen_token()
        campaign_ids = get_campaign_ids(dt)

        if not campaign_ids:
            raise Exception("无有效campaign_id，任务终止")

        all_data = []
        for cid in campaign_ids:
            all_data.extend(get_campaign_target_info(token, cid))

        # 数据格式化
        write_data = [[
            t["request_campaign_id"], t["result_campaign_id"],
            t["target_display"], t["update_time"], t["target_name"],
            t["target_id"], t["full_request_url"],
            t["pre_parse_raw_text"], t["etl_datetime"]
        ] for t in all_data]

        write_to_odps(TABLE_NAMES["target_info"], write_data, dt)

    except Exception as e:
        print(f"[{get_log()}] 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()