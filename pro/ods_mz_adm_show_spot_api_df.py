# -*- coding: utf-8 -*-
"""
秒针Campaign广告位详情采集脚本
功能：采集数据并调用通用ODPS写入函数写入数据
核心：完全复用你提供的write_to_odps函数
数据来源：/cms/v1/campaigns/show_spot 接口
依赖：odps库（pip install odps）
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS

# ===================== 全局配置 & 初始化 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 全局ODPS项目变量（自动获取）
ODPS_PROJECT = ODPS().project

# 核心配置
CONFIG = {
    # MaxCompute配置
    "odps": {
        "table_name": "ods_mz_adm_show_spot_api_df",  # 目标表名
        "partition_col": "dt",  # 分区字段（按天）
        # 表字段列表（需与实际表结构一致，顺序匹配）
        "table_columns": [
            "request_campaign_id",
            "request_spot_id_str",
            "publisher_id",
            "channel_name",
            "publisher_name",
            "spot_id",
            "GUID",
            "description",
            "CAGUID",
            "customize",
            "report_metrics",
            "market",
            "vending_model",
            "area_size",
            "linked_siteid",
            "placement_name",
            "spot_id_str",
            "keyword",  # 新增：关键词列表（JSON格式）
            "adposition_type",
            "pre_parse_raw_text",
            "etl_datetime"
        ]
    },
    # API配置
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "campaign_list_spots_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_spots",
        "campaign_show_spot_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/show_spot",
        "timeout": 30,
        "interval": 0.02
    },
    "batch_size": 1000  # 批量写入大小
}


# ====================== 通用ODPS写入函数（完全复用你提供的版本） ======================
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


# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_partition_date() -> str:
    """获取分区日期（yyyyMMdd）"""
    return datetime.now().strftime("%Y%m%d")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None/-"""
    if value is None or value == "" or str(value).lower() == "null" or value == "-":
        return ""
    return str(value)


# ===================== Access Token获取 =====================
def get_access_token() -> str:
    """获取OAuth Access Token"""
    try:
        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=CONFIG["api"]["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()

        access_token = token_data.get("access_token")
        if not access_token:
            raise Exception(f"Token返回无access_token字段，返回数据：{token_data}")

        print(f"✅ Access Token获取成功（前10位：{access_token[:10]}...）")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Access Token失败：{str(e)}")


# ===================== 获取所有campaign_id =====================
def get_campaign_ids(token: str) -> List[str]:
    """从/cms/v1/campaigns/list接口获取所有campaign_id"""
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        # 解析campaign_id
        campaign_ids = []
        if isinstance(raw_data, list):
            campaign_ids = [to_string(item.get("campaign_id")) for item in raw_data if item.get("campaign_id")]
        elif isinstance(raw_data, dict):
            data_nodes = ["result", "list", "data", "campaigns"]
            for node in data_nodes:
                if node in raw_data and isinstance(raw_data[node], list):
                    campaign_ids = [to_string(item.get("campaign_id")) for item in raw_data[node] if
                                    item.get("campaign_id")]
                    break
            if not campaign_ids and raw_data.get("campaign_id"):
                campaign_ids = [to_string(raw_data.get("campaign_id"))]

        # 去重+过滤空值
        campaign_ids = list(set([cid for cid in campaign_ids if cid]))
        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        time.sleep(CONFIG["api"]["interval"])
        return campaign_ids
    except Exception as e:
        raise Exception(f"获取campaign_id失败：{str(e)}")


# ===================== 获取单个campaign的spot_id_str列表 =====================
def get_spot_id_str_list(token: str, campaign_id: str) -> List[str]:
    """从/cms/v1/campaigns/list_spots接口获取单个campaign的spot_id_str列表"""
    try:
        params = {
            "campaign_id": campaign_id,
            "access_token": token
        }
        resp = requests.get(
            CONFIG["api"]["campaign_list_spots_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spots_data = resp.json()

        spot_id_str_list = []
        if isinstance(spots_data, list):
            spot_id_str_list = [to_string(item.get("spot_id_str")) for item in spots_data if item.get("spot_id_str")]
        elif isinstance(spots_data, dict) and "result" in spots_data:
            spot_id_str_list = [to_string(item.get("spot_id_str")) for item in spots_data["result"] if
                                item.get("spot_id_str")]

        spot_id_str_list = list(set([sid for sid in spot_id_str_list if sid]))
        print(f"✅ campaign_id={campaign_id} 获取到{len(spot_id_str_list)}个有效spot_id_str")
        time.sleep(CONFIG["api"]["interval"])
        return spot_id_str_list
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id} 获取spot_id_str失败，跳过：{str(e)}")
        return []


# ===================== 采集单个广告位详情 =====================
def get_spot_detail(token: str, campaign_id: str, spot_id_str: str) -> Optional[Dict]:
    """采集单个campaign_id+spot_id_str对应的广告位详情"""
    try:
        params = {
            "campaign_id": campaign_id,
            "spot_id_str": spot_id_str,
            "access_token": token,
            "keyword": "on"  # 新增必传参数 keyword=on
        }
        resp = requests.get(
            CONFIG["api"]["campaign_show_spot_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_detail = resp.json()

        if not isinstance(spot_detail, dict):
            print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 返回非字典格式，跳过")
            return None

        etl_datetime = get_etl_datetime()
        # 标准化字段（仅保留表中有的字段）
        standard_spot = {}
        for col in CONFIG["odps"]["table_columns"]:
            if col == "request_campaign_id":
                standard_spot[col] = to_string(campaign_id)
            elif col == "request_spot_id_str":
                standard_spot[col] = to_string(spot_id_str)
            elif col == "etl_datetime":
                standard_spot[col] = etl_datetime
            # 新增：处理keyword字段（接口返回的keyword或空字符串）
            elif col == "keyword":
                standard_spot[col] = to_string(spot_detail.get(col, ""))
            else:
                standard_spot[col] = to_string(spot_detail.get(col, ""))

        # 补充原始文本字段
        standard_spot["pre_parse_raw_text"] = to_string(json.dumps(spot_detail, ensure_ascii=False))

        print(f"✅ campaign_id={campaign_id}, spot_id_str={spot_id_str} 采集成功")
        time.sleep(CONFIG["api"]["interval"])
        return standard_spot
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 采集失败，跳过：{str(e)}")
        return None


# ===================== 数据转换（适配通用写入函数） =====================
def convert_data_to_list(data_list: List[Dict]) -> List[List]:
    """
    将字典格式数据转换为列表格式（适配write_to_odps函数）
    :param data_list: 字典格式的采集数据
    :return: 列表格式的写入数据
    """
    write_data = []
    for data in data_list:
        row = []
        # 按表字段顺序组装数据（不含分区字段）
        for col in CONFIG["odps"]["table_columns"]:
            row.append(data.get(col, ""))
        write_data.append(row)
    return write_data


# ===================== 主流程 =====================
def main():
    try:
        print("=" * 80)
        print("🚀 秒针广告位详情采集+通用ODPS写入任务启动")
        print(f"🔧 全局ODPS项目：{ODPS_PROJECT}")
        print("=" * 80)

        # 1. 前置校验
        if not ODPS_PROJECT:
            raise Exception("❌ 全局ODPS_PROJECT变量获取失败，请检查ODPS环境配置")

        # 2. 获取Access Token
        token = get_access_token()

        # 3. 获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            print("⚠️ 未获取到任何campaign_id，任务终止")
            return

        # 4. 遍历采集数据
        all_spot_detail_data = []
        target_table = CONFIG["odps"]["table_name"]
        partition_dt = get_partition_date()

        for campaign_id in campaign_ids:
            # 获取spot_id_str列表
            spot_id_str_list = get_spot_id_str_list(token, campaign_id)
            if not spot_id_str_list:
                continue

            # 采集每个spot详情
            for spot_id_str in spot_id_str_list:
                spot_detail = get_spot_detail(token, campaign_id, spot_id_str)
                if spot_detail:
                    all_spot_detail_data.append(spot_detail)

                    # 达到批量大小则调用通用写入函数
                    if len(all_spot_detail_data) >= CONFIG["batch_size"]:
                        # 转换数据格式
                        write_data = convert_data_to_list(all_spot_detail_data)
                        # 调用通用写入函数
                        write_to_odps(target_table, write_data, partition_dt)
                        print(f"✅ 批量写入{len(write_data)}条数据完成")
                        all_spot_detail_data = []

        # 写入剩余数据
        if all_spot_detail_data:
            write_data = convert_data_to_list(all_spot_detail_data)
            write_to_odps(target_table, write_data, partition_dt)
            print(f"✅ 剩余数据{len(write_data)}条写入完成")

        print("\n" + "=" * 80)
        print(f"✅ 任务完成！数据已写入 {ODPS_PROJECT}.{target_table}（分区：dt='{partition_dt}'）")
        print("=" * 80)

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()