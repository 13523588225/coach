# -*- coding: utf-8 -*-
"""
秒针TV监测活动目标信息采集脚本
数据来源：/monitortv/v1/campaign/target/info 接口
依赖campaign_id：/monitortv/v1/campaigns/list 接口
Token来源：https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "campaign_target_info_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaign/target/info",
    "timeout": 30,
    "request_interval": 0.2  # 接口调用间隔，避免限流
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "target_info": "ods_mz_tvm_target_info_api_df"  # 目标表名
}


# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_log() -> str:
    """获取日志时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


# ===================== 从MaxCompute查询API账号密码 =====================
def get_tvm_api_credentials():
    """
    从数仓表 ods_mz_user_api_df 查询TVM接口的账号密码
    """
    o = ODPS(project=ODPS_PROJECT)
    sql = """
    select username, passwords 
    from ods_mz_user_api_df 
    where api_source = 'TVM'
    limit 1
    """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_log()}] 🔐 成功从数仓获取TVM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"❌ 查询账号密码失败：{str(e)}")


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取秒针TV监测Token"""
    try:
        # 从数仓获取账号密码
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


def get_campaign_ids(token: str) -> List[str]:
    """从campaigns/list接口获取所有campaign_id"""
    try:
        resp = requests.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])
        # 提取并去重campaign_id
        campaign_ids = list(set([to_string(c.get("campaign_id")) for c in campaigns if isinstance(c, dict)]))
        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        return campaign_ids
    except Exception as e:
        raise Exception(f"获取campaign_id失败：{str(e)}")


def get_campaign_target_info(token: str, request_campaign_id: str) -> List[Dict]:
    """
    获取单个campaign_id对应的活动目标信息
    记录：调用参数campaign_id + result所有字段
    """
    try:
        # 接口调用参数（仅记录campaign_id）
        params = {
            "access_token": token,
            "campaign_id": request_campaign_id
        }
        # 构造完整请求URL
        full_request_url = f"{API_CONFIG['campaign_target_info_url']}?access_token={token}&campaign_id={request_campaign_id}"
        # 调用目标信息接口
        resp = requests.get(
            API_CONFIG["campaign_target_info_url"],
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        target_data = resp.json()

        # 接口错误码校验
        if target_data.get("error_code") != 0:
            print(f"⚠️ 请求参数campaign_id={request_campaign_id} 采集失败：{target_data.get('error_message')}")
            return []

        result = target_data.get("result", {})
        if not isinstance(result, dict):
            print(f"⚠️ 请求参数campaign_id={request_campaign_id} 返回格式异常")
            return []

        # 解析嵌套的target数组，关联请求参数
        target_list = result.get("target", [])
        etl_datetime = get_etl_datetime()
        standard_target_list = []

        for target in target_list:
            if not isinstance(target, dict):
                continue
            # 核心字段：请求参数 + result所有字段
            standard_target = {
                # 调用参数（仅campaign_id）
                "request_campaign_id": to_string(request_campaign_id),

                # result顶层字段
                "result_campaign_id": to_string(result.get("campaign_id")),

                # result.target数组内字段
                "target_display": to_string(target.get("target_display")),
                "update_time": to_string(target.get("update_time")),
                "target_name": to_string(target.get("target_name")),
                "target_id": to_string(target.get("target_id")),

                # 新增字段：完整接口请求URL
                "full_request_url": to_string(full_request_url),

                # 补充溯源字段
                "pre_parse_raw_text": to_string(json.dumps(result, ensure_ascii=False)),
                "etl_datetime": to_string(etl_datetime)
            }
            standard_target_list.append(standard_target)

        print(f"✅ 请求参数campaign_id={request_campaign_id} 采集到{len(standard_target_list)}条目标信息")
        time.sleep(API_CONFIG["request_interval"])
        return standard_target_list
    except Exception as e:
        print(f"⚠️ 请求参数campaign_id={request_campaign_id} 采集异常：{str(e)}")
        time.sleep(API_CONFIG["request_interval"])
        return []


# ===================== ODPS写入 =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    """通用ODPS写入函数（清空分区+写入）"""
    if not data:
        print(f"⚠️ {table_name} 无数据可写入")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表{table_name}不存在")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    # 清空分区（防重复）
    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{table_name}.{partition_spec}")

    # 写入数据
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入成功：{table_name} | 分区{dt} | 条数{len(data)}")


# ===================== 主流程 =====================
def main():
    try:
        # 1. 获取Token
        token = get_miaozhen_token()
        print(f"✅ Token获取成功")

        # 2. 获取所有campaign_id（作为请求参数）
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            raise Exception("未获取到任何campaign_id，任务终止")

        # 3. 遍历采集所有campaign_id的目标信息
        all_target_data = []
        for request_campaign_id in campaign_ids:
            target_data = get_campaign_target_info(token, request_campaign_id)
            if target_data:
                all_target_data.extend(target_data)

        print(f"✅ 累计采集到{len(all_target_data)}条活动目标信息")

        # 4. 格式化写入数据（与表字段顺序严格一致）
        target_write_data = [
            [
                t["request_campaign_id"],
                t["result_campaign_id"],
                t["target_display"],
                t["update_time"],
                t["target_name"],
                t["target_id"],
                t["full_request_url"],
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in all_target_data
        ]

        # 5. 写入ODPS（分区日期可替换为动态参数）
        write_to_odps(TABLE_NAMES["target_info"], target_write_data, args['dt'])

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()