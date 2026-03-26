'''PyODPS 3
请确保不要使用从 MaxCompute下载数据来处理。下载数据操作常包括Table/Instance的open_reader以及 DataFrame的to_pandas方法。
推荐使用 MaxFrame DataFrame（从 MaxCompute 表创建）来处理数据，MaxFrame DataFrame数据计算发生在MaxCompute集群，无需拉数据至本地。
MaxFrame相关介绍及使用可参考：https://help.aliyun.com/zh/maxcompute/user-guide/maxframe
'''
# -*- coding: utf-8 -*-
"""
秒针TV监测广告点位列表采集脚本
数据来源：/monitortv/v1/spot/list 接口
依赖campaign_id：/monitortv/v1/campaigns/list 接口
Token来源：https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get
"""
import requests
import json
import time
import urllib3
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "spot_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/list",
    "timeout": 30,
    "request_interval": 0.2  # 接口调用间隔，避免限流
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "spot_list": "ods_mz_tvm_spot_list_api_df"  # 广告点位列表目标表
}


# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


def get_log() -> str:
    """获取日志时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
        # 从数仓动态获取账号密码
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


def get_spot_list(token: str, request_campaign_id: str) -> List[Dict]:
    """
    获取单个campaign_id对应的广告点位列表
    记录：调用参数campaign_id + result数组内所有字段
    """
    try:
        # 接口调用参数（核心：campaign_id）
        params = {
            "access_token": token,
            "campaign_id": request_campaign_id
        }
        # 构建完整请求URL
        full_request_url = requests.compat.urljoin(
            API_CONFIG["spot_list_url"],
            f"?{urllib.parse.urlencode(params, safe='=&')}"
        )
        resp = requests.get(
            API_CONFIG["spot_list_url"],
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_list_data = resp.json()

        # 接口错误码校验
        if spot_list_data.get("error_code") != 0:
            print(f"⚠️ campaign_id={request_campaign_id} 点位列表采集失败：{spot_list_data.get('error_message')}")
            return []

        result = spot_list_data.get("result", [])
        if not isinstance(result, list):
            print(f"⚠️ campaign_id={request_campaign_id} 点位列表格式异常（非数组）")
            return []

        etl_datetime = get_etl_datetime()
        standard_spot_list = []
        # 解析result数组，每条记录关联请求参数campaign_id
        for spot in result:
            if not isinstance(spot, dict):
                continue
            standard_spot = {
                # 调用参数（核心）
                "request_campaign_id": to_string(request_campaign_id),

                # result数组内所有字段（全量覆盖）
                "spid_str": to_string(spot.get("spid_str")),
                "caid": to_string(spot.get("caid")),
                "created_time": to_string(spot.get("created_time")),
                "placement": to_string(spot.get("placement")),
                "channel_name": to_string(spot.get("channel_name")),
                "pub_id": to_string(spot.get("pub_id")),
                "publisher": to_string(spot.get("publisher")),
                "description": to_string(spot.get("description")),
                "ad_type": to_string(spot.get("ad_type")),
                "category": to_string(spot.get("category")),
                "brand": to_string(spot.get("brand")),
                "product": to_string(spot.get("product")),
                "copyname": to_string(spot.get("copyname")),
                "buyingregular": to_string(spot.get("buyingregular")),
                "buyingsub": to_string(spot.get("buyingsub")),
                "mediasub": to_string(spot.get("mediasub")),
                "generalbuying": to_string(spot.get("generalbuying")),
                "spottype": to_string(spot.get("spottype")),
                "spotplan": to_string(spot.get("spotplan")),
                "spot_plan_record_id": to_string(spot.get("spot_plan_record_id")),
                "caguid": to_string(spot.get("caguid")),
                "celebrity_stids": to_string(spot.get("celebrity_stids")),
                "playinfo_stids": to_string(spot.get("playinfo_stids")),
                "spots_display_type": to_string(spot.get("spots_display_type")),
                "purchasetype": to_string(spot.get("purchasetype")),
                "playpurchasetype": to_string(spot.get("playpurchasetype")),
                "mm_channe_id": to_string(spot.get("mm_channe_id")),
                "play_info": to_string(spot.get("play_info")),
                "tag_place": to_string(spot.get("tag_place")),
                "multi_tag": to_string(spot.get("multi_tag")),

                # 新增字段：完整接口请求URL
                "full_request_url": to_string(full_request_url),

                # 补充溯源字段
                "pre_parse_raw_text": to_string(json.dumps(spot, ensure_ascii=False)),
                "etl_datetime": to_string(etl_datetime)
            }
            standard_spot_list.append(standard_spot)

        print(f"✅ campaign_id={request_campaign_id} 采集到{len(standard_spot_list)}个点位列表数据")
        time.sleep(API_CONFIG["request_interval"])
        return standard_spot_list
    except Exception as e:
        print(f"⚠️ campaign_id={request_campaign_id} 点位列表采集异常：{str(e)}")
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

        # 2. 获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            raise Exception("未获取到任何campaign_id，任务终止")

        # 3. 遍历采集所有campaign_id的点位列表
        all_spot_list_data = []
        for request_campaign_id in campaign_ids:
            spot_list_data = get_spot_list(token, request_campaign_id)
            if spot_list_data:
                all_spot_list_data.extend(spot_list_data)

        print(f"✅ 累计采集到{len(all_spot_list_data)}个广告点位列表数据")

        # 4. 格式化写入数据（与表字段顺序严格一致）
        spot_list_write_data = [
            [
                # 调用参数
                t["request_campaign_id"],

                # result数组内所有字段
                t["spid_str"],
                t["caid"],
                t["created_time"],
                t["placement"],
                t["channel_name"],
                t["pub_id"],
                t["publisher"],
                t["description"],
                t["ad_type"],
                t["category"],
                t["brand"],
                t["product"],
                t["copyname"],
                t["buyingregular"],
                t["buyingsub"],
                t["mediasub"],
                t["generalbuying"],
                t["spottype"],
                t["spotplan"],
                t["spot_plan_record_id"],
                t["caguid"],
                t["celebrity_stids"],
                t["playinfo_stids"],
                t["spots_display_type"],
                t["purchasetype"],
                t["playpurchasetype"],
                t["mm_channe_id"],
                t["play_info"],
                t["tag_place"],
                t["multi_tag"],

                # 新增的full_request_url字段
                t["full_request_url"],

                # 溯源字段
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in all_spot_list_data
        ]

        # 5. 写入ODPS（分区日期可替换为动态参数）
        write_to_odps(TABLE_NAMES["spot_list"], spot_list_write_data, args['dt'])

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()