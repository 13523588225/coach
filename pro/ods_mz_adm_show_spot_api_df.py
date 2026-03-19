# -*- coding: utf-8 -*-
"""
秒针Campaign广告位详情采集脚本
数据来源：/cms/v1/campaigns/show_spot 接口
依赖spot_id_str：/cms/v1/campaigns/list_spots 接口
依赖campaign_id：/cms/v1/campaigns/list 接口
Token来源：https://api.cn.miaozhen.com/oauth/token
目标表：ods_mz_adm_campaigns_show_spot_api_df
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
        "campaign_list_spots_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_spots",  # 获取spot_id_str
        "campaign_show_spot_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/show_spot",  # 广告位详情接口
        "timeout": 30,
        "interval": 0.2  # 接口调用间隔
    },
    "table_name": "ods_mz_adm_campaigns_show_spot_api_df",  # 目标表名
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

        # 适配常见返回格式（数组/字典含result节点）
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


# ===================== 获取单个campaign的spot_id_str列表 =====================
def get_spot_id_str_list(token: str, campaign_id: str) -> List[str]:
    """从/cms/v1/campaigns/list_spots接口获取单个campaign的spot_id_str列表"""
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        params = {"campaign_id": campaign_id}
        resp = requests.get(
            CONFIG["api"]["campaign_list_spots_url"],
            params=params,
            headers=headers,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spots_data = resp.json()

        # 适配常见返回格式（数组/字典含result节点）
        spot_id_str_list = []
        if isinstance(spots_data, list):
            spot_id_str_list = [to_string(item.get("spot_id_str")) for item in spots_data if item.get("spot_id_str")]
        elif isinstance(spots_data, dict) and "result" in spots_data:
            spot_id_str_list = [to_string(item.get("spot_id_str")) for item in spots_data["result"] if
                                item.get("spot_id_str")]

        # 去重并过滤空值
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
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        # 携带双参数调用详情接口
        params = {
            "campaign_id": campaign_id,
            "spot_id_str": spot_id_str
        }
        resp = requests.get(
            CONFIG["api"]["campaign_show_spot_url"],
            params=params,
            headers=headers,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_detail = resp.json()

        # 校验返回格式为字典
        if not isinstance(spot_detail, dict):
            print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 返回非字典格式，跳过：{spot_detail}")
            return None

        etl_datetime = get_etl_datetime()
        # 标准化所有字段，关联调用参数
        standard_spot = {
            # 调用参数（核心双参数）
            "request_campaign_id": to_string(campaign_id),
            "request_spot_id_str": to_string(spot_id_str),

            # 接口返回所有字段（全量覆盖）
            "publisher_id": to_string(spot_detail.get("publisher_id")),
            "channel_name": to_string(spot_detail.get("channel_name")),
            "publisher_name": to_string(spot_detail.get("publisher_name")),
            "spot_id": to_string(spot_detail.get("spot_id")),
            "GUID": to_string(spot_detail.get("GUID")),
            "description": to_string(spot_detail.get("description")),
            "CAGUID": to_string(spot_detail.get("CAGUID")),
            "customize": to_string(spot_detail.get("customize")),
            "report_metrics": to_string(spot_detail.get("report_metrics")),
            "market": to_string(spot_detail.get("market")),
            "vending_model": to_string(spot_detail.get("vending_model")),
            "area_size": to_string(spot_detail.get("area_size")),
            "linked_siteid": to_string(spot_detail.get("linked_siteid")),
            "placement_name": to_string(spot_detail.get("placement_name")),
            "spot_id_str": to_string(spot_detail.get("spot_id_str")),
            "adposition_type": to_string(spot_detail.get("adposition_type")),

            # 溯源字段
            "pre_parse_raw_text": to_string(json.dumps(spot_detail, ensure_ascii=False)),
            "etl_datetime": to_string(etl_datetime)
        }
        print(f"✅ campaign_id={campaign_id}, spot_id_str={spot_id_str} 广告位详情采集成功")
        time.sleep(CONFIG["api"]["interval"])
        return standard_spot
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 采集失败，跳过：{str(e)}")
        return None


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

        # 3. 遍历采集所有广告位详情
        all_spot_detail_data = []
        for campaign_id in campaign_ids:
            # 3.1 获取当前campaign的spot_id_str列表
            spot_id_str_list = get_spot_id_str_list(token, campaign_id)
            if not spot_id_str_list:
                continue

            # 3.2 遍历采集每个spot_id_str的详情
            for spot_id_str in spot_id_str_list:
                spot_detail = get_spot_detail(token, campaign_id, spot_id_str)
                if spot_detail:
                    all_spot_detail_data.append(spot_detail)

        if not all_spot_detail_data:
            print("⚠️ 未采集到任何广告位详情数据，任务终止")
            return
        print(f"✅ 累计采集到{len(all_spot_detail_data)}条广告位详情数据")

        # 4. 格式化数据（与表字段顺序严格一致）
        write_data = [
            [
                # 调用参数
                t["request_campaign_id"],
                t["request_spot_id_str"],

                # 接口返回字段
                t["publisher_id"],
                t["channel_name"],
                t["publisher_name"],
                t["spot_id"],
                t["GUID"],
                t["description"],
                t["CAGUID"],
                t["customize"],
                t["report_metrics"],
                t["market"],
                t["vending_model"],
                t["area_size"],
                t["linked_siteid"],
                t["placement_name"],
                t["spot_id_str"],
                t["adposition_type"],

                # 溯源字段
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in all_spot_detail_data
        ]

        # 5. 批量写入ODPS（分区日期可替换为动态参数args['dt']）
        batch_write_to_odps(write_data, '20260318')

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()