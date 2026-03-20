# -*- coding: utf-8 -*-
"""
秒针Campaign目标列表采集脚本 - 最终精简版
核心逻辑：URL参数鉴权 + 数据直接写入MaxCompute + 极简ODPS配置
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. API配置
API_CONFIG = {
    "token_url": "https://api.cn.miaozhen.com/oauth/token",
    "auth": {
        "grant_type": "password",
        "username": "Coach_api",
        "password": "Coachapi2026",
        "client_id": "COACH2026_API",
        "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
    },
    "campaign_list_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
    "campaign_targets_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_targets",
    "timeout": 30,
    "interval": 0.2
}

# 2. 目标表配置
TABLE_CONFIG = {
    "table_name": "ods_mz_adm_list_targets_api_df",
    "dt": datetime.now().strftime("%Y%m%d")  # 动态分区（当日日期）
}

# ===================== 全局ODPS项目变量（仅保留核心定义） =====================
ODPS_PROJECT = ODPS().project


# ===================== 工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


# ===================== 通用ODPS写入函数（完全复用你提供的版本） =====================
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


# ===================== Access Token获取 =====================
def get_access_token() -> str:
    """获取OAuth Access Token"""
    try:
        resp = requests.post(
            API_CONFIG["token_url"],
            data=API_CONFIG["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise Exception(f"Token返回无access_token字段：{token_data}")

        print(f"✅ Access Token获取成功")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


# ===================== 获取所有campaign_id =====================
def get_campaign_ids(access_token: str) -> List[str]:
    """从/cms/v1/campaigns/list接口获取campaign_id（URL参数鉴权）"""
    try:
        params = {"access_token": access_token}
        resp = requests.get(
            API_CONFIG["campaign_list_url"],
            params=params,
            headers={"Content-Type": "application/json"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaign_data = resp.json()

        # 解析campaign_id
        campaign_ids = []
        if isinstance(campaign_data, list):
            campaign_ids = [to_string(item.get("campaign_id")) for item in campaign_data if item.get("campaign_id")]
        elif isinstance(campaign_data, dict) and "result" in campaign_data:
            campaign_ids = [to_string(item.get("campaign_id")) for item in campaign_data["result"] if
                            item.get("campaign_id")]

        # 去重+过滤空值
        campaign_ids = list(set([cid for cid in campaign_ids if cid]))
        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        time.sleep(API_CONFIG["interval"])
        return campaign_ids
    except Exception as e:
        raise Exception(f"获取campaign_id失败：{str(e)}")


# ===================== 采集单个campaign的目标列表 =====================
def get_campaign_targets(access_token: str, campaign_id: str) -> List[Dict]:
    """采集单个campaign_id的目标列表数据（仅返回数据，不打印）"""
    try:
        params = {
            "access_token": access_token,
            "campaign_id": campaign_id
        }
        resp = requests.get(
            API_CONFIG["campaign_targets_url"],
            params=params,
            headers={"Content-Type": "application/json"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        targets_data = resp.json()

        # 校验返回格式
        if not isinstance(targets_data, list):
            return []

        # 标准化数据
        etl_datetime = get_etl_datetime()
        standard_targets = []
        for target in targets_data:
            if not isinstance(target, dict):
                continue

            standard_target = {
                "request_campaign_id": campaign_id,
                "panel_id": to_string(target.get("panel_id")),
                "target_id": to_string(target.get("target_id")),
                "target_name": to_string(target.get("target_name")),
                "pre_parse_raw_text": to_string(json.dumps(target, ensure_ascii=False)),
                "etl_datetime": etl_datetime
            }
            standard_targets.append(standard_target)

        time.sleep(API_CONFIG["interval"])
        return standard_targets
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id} 采集失败，跳过：{str(e)}")
        return []


# ===================== 主流程 =====================
def main():
    try:
        # 1. 获取Access Token
        access_token = get_access_token()
        if not access_token:
            print("⚠️ Token为空，任务终止")
            return

        # 2. 获取所有campaign_id
        campaign_ids = get_campaign_ids(access_token)
        if not campaign_ids:
            print("⚠️ 未获取到campaign_id，任务终止")
            return

        # 3. 批量采集目标列表数据
        all_targets = []
        for cid in campaign_ids:
            targets = get_campaign_targets(access_token, cid)
            all_targets.extend(targets)

        if not all_targets:
            print("⚠️ 未采集到任何目标列表数据，任务终止")
            return

        # 4. 格式化入库数据（与表字段顺序严格一致）
        write_data = [
            [
                t["request_campaign_id"],
                t["panel_id"],
                t["target_id"],
                t["target_name"],
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in all_targets
        ]

        # 5. 写入MaxCompute（调用通用写入函数）
        write_to_odps(
            table_name=TABLE_CONFIG["table_name"],
            data=write_data,
            dt=TABLE_CONFIG["dt"]
        )

        print(
            f"🎉 任务执行完成：累计采集{len(all_targets)}条数据，已写入{TABLE_CONFIG['table_name']}（分区{TABLE_CONFIG['dt']}）")

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()