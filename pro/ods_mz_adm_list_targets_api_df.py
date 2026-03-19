# -*- coding: utf-8 -*-
"""
秒针Campaign目标列表采集脚本
数据来源：/cms/v1/campaigns/list_targets 接口
依赖campaign_id：/cms/v1/campaigns/list 接口
Token来源：https://api.cn.miaozhen.com/oauth/token
目标表：ods_mz_adm_campaigns_list_targets_api_df
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS

# ===================== 统一配置管理 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    "api": {
        # Token相关配置
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        # 接口相关配置
        "campaign_list_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",  # 获取campaign_id
        "campaign_targets_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_targets",  # 目标列表接口
        "timeout": 30,
        "interval": 0.2  # 接口调用间隔
    },
    "table_name": "ods_mz_adm_list_targets_api_df",  # 目标表名
    "batch_size": 1000  # ODPS批量写入大小
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
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.get(
            CONFIG["api"]["campaign_list_url"],
            headers=headers,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaign_data = resp.json()

        # 适配campaign_list接口的返回格式（假设返回数组/包含campaign_id的字典）
        campaign_ids = []
        if isinstance(campaign_data, list):
            campaign_ids = [to_string(item.get("campaign_id")) for item in campaign_data if item.get("campaign_id")]
        elif isinstance(campaign_data, dict) and "result" in campaign_data:
            campaign_ids = [to_string(item.get("campaign_id")) for item in campaign_data["result"] if
                            item.get("campaign_id")]

        # 去重并过滤空值
        campaign_ids = list(set([cid for cid in campaign_ids if cid]))
        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        time.sleep(CONFIG["api"]["interval"])
        return campaign_ids
    except Exception as e:
        raise Exception(f"获取campaign_id失败：{str(e)}")


# ===================== 采集单个campaign的目标列表 =====================
def get_campaign_targets(token: str, campaign_id: str) -> List[Dict]:
    """采集单个campaign_id对应的目标列表数据"""
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        # 携带campaign_id参数（GET请求拼URL，POST可改data/json）
        params = {"campaign_id": campaign_id}
        resp = requests.get(
            CONFIG["api"]["campaign_targets_url"],
            params=params,
            headers=headers,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        targets_data = resp.json()

        # 校验返回格式为数组
        if not isinstance(targets_data, list):
            print(f"⚠️ campaign_id={campaign_id} 目标列表返回非数组格式，跳过：{targets_data}")
            return []

        etl_datetime = get_etl_datetime()
        standard_targets = []
        # 解析每条目标数据，关联campaign_id参数
        for idx, target in enumerate(targets_data):
            if not isinstance(target, dict):
                print(f"⚠️ campaign_id={campaign_id} 第{idx + 1}条数据非字典，跳过")
                continue

            standard_target = {
                # 调用参数（核心）
                "request_campaign_id": to_string(campaign_id),
                # 接口返回所有字段
                "panel_id": to_string(target.get("panel_id")),
                "target_id": to_string(target.get("target_id")),
                "target_name": to_string(target.get("target_name")),
                # 溯源字段
                "pre_parse_raw_text": to_string(json.dumps(target, ensure_ascii=False)),
                "etl_datetime": to_string(etl_datetime)
            }
            standard_targets.append(standard_target)

        print(f"✅ campaign_id={campaign_id} 采集到{len(standard_targets)}条目标列表数据")
        time.sleep(CONFIG["api"]["interval"])
        return standard_targets
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id} 采集失败，跳过：{str(e)}")
        return []


# ===================== ODPS批量写入 =====================
def batch_write_to_odps(data: List[List], dt: str):
    """按batch_size分批次写入ODPS"""
    if not data:
        print(f"⚠️ 无有效数据写入ODPS，任务终止")
        return

    try:
        o = ODPS()
        table_name = CONFIG["table_name"]
        table = o.get_table(table_name)
        partition_spec = f"dt='{dt}'"
        batch_size = CONFIG["batch_size"]

        # 清空分区防止重复
        if table.exist_partition(partition_spec):
            o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{table_name} {partition_spec}")

        # 分批次写入
        total_count = len(data)
        batches = [data[i:i + batch_size] for i in range(0, total_count, batch_size)]

        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for batch_idx, batch in enumerate(batches):
                writer.write(batch)
                print(f"✅ 写入第{batch_idx + 1}/{len(batches)}批次，条数：{len(batch)}")

        print(f"🎉 数据全部写入完成：{table_name} | 分区{dt} | 总条数{total_count}")
    except Exception as e:
        raise Exception(f"ODPS批量写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    try:
        # 1. 获取Access Token
        token = get_access_token()

        # 2. 获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            print("⚠️ 未获取到任何campaign_id，任务终止")
            return

        # 3. 遍历采集所有campaign的目标列表
        all_targets_data = []
        for campaign_id in campaign_ids:
            targets_data = get_campaign_targets(token, campaign_id)
            if targets_data:
                all_targets_data.extend(targets_data)

        if not all_targets_data:
            print("⚠️ 未采集到任何目标列表数据，任务终止")
            return
        print(f"✅ 累计采集到{len(all_targets_data)}条目标列表数据")

        # 4. 格式化数据（与表字段顺序严格一致）
        write_data = [
            [
                t["request_campaign_id"],
                t["panel_id"],
                t["target_id"],
                t["target_name"],
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in all_targets_data
        ]

        # 5. 批量写入ODPS（分区日期可替换为动态参数args['dt']）
        batch_write_to_odps(write_data, '20260318')

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()