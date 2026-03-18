# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置（ODPS自动获取） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "list_spots_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_spots",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 30,
        "interval": 0.1,
        "campaign_batch_size": 20  # 活动批量采集大小
    },
    "table_name": "ods_mz_adm_cms_campaigns_list_spots_api_df",
    "batch_size": 1000
}

# 自动获取ODPS项目名
try:
    ODPS_PROJECT = ODPS().project
    if not ODPS_PROJECT:
        raise Exception("ODPS项目名自动获取失败，请检查ODPS客户端配置")
    print(f"✅ 自动获取ODPS项目名：{ODPS_PROJECT}")
except Exception as e:
    raise Exception(f"ODPS项目初始化失败：{str(e)}")


# ===================== 内置工具函数 =====================
def safe_str(val):
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_miaozhen_token():
    print("🔍 获取秒针Token...")
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
        token = token_data.get("access_token")
        if not token:
            raise Exception(f"Token为空：{json.dumps(token_data, ensure_ascii=False)}")
        print("✅ Token获取成功")
        return token
    except Exception as e:
        raise Exception(f"Token获取失败：{str(e)}")


def get_campaign_ids(token):
    """获取所有活动ID"""
    print("🔍 获取活动ID列表...")
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()
        campaigns = raw_data.get("result", {}).get("campaigns", raw_data)
        campaign_ids = [camp.get("campaign_id") for camp in campaigns
                        if isinstance(camp, dict) and camp.get("campaign_id")]
        print(f"✅ 获取到{len(campaign_ids)}个活动ID")
        return campaign_ids
    except Exception as e:
        raise Exception(f"活动ID获取失败：{str(e)}")


def init_odps_client():
    try:
        odps = ODPS(project=ODPS_PROJECT)
        options.tunnel.use_instance_tunnel = True
        options.read_timeout = 300
        options.connect_timeout = 60
        options.tunnel.limit_instance_tunnel = False
        print(f"✅ ODPS初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def write_odps_data(odps, data, partition_dt):
    if not data:
        print(f"⚠️ 表{CONFIG['table_name']}无数据可写入")
        return

    try:
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"表{CONFIG['table_name']}不存在")

        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{partition_dt}'"

        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {CONFIG['table_name']} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for i in range(0, len(data), CONFIG["batch_size"]):
                batch_data = data[i:i + CONFIG["batch_size"]]
                writer.write(batch_data)
                print(f"🔸 写入批次{i // CONFIG['batch_size'] + 1} | 条数：{len(batch_data)}")

        print(f"✅ 表{CONFIG['table_name']}写入完成 | 总条数：{len(data)}")
    except Exception as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ===================== 核心业务逻辑 =====================
def collect_spot_data(token, campaign_id):
    """采集单个活动广告位"""
    try:
        params = {"access_token": token, "campaign_id": campaign_id}
        resp = requests.get(
            CONFIG["api"]["list_spots_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        pre_parse_raw_text = resp.text
        raw_data = resp.json()

        spots = raw_data.get("result", {}).get("spots", [])
        valid_spots = []
        for spot in spots:
            if isinstance(spot, dict) and spot.get("spot_id"):
                spot["campaign_id"] = campaign_id
                spot["pre_parse_raw_text"] = pre_parse_raw_text
                valid_spots.append(spot)

        print(f"  ✅ 活动{campaign_id}采集到{len(valid_spots)}个广告位")
        return valid_spots
    except Exception as e:
        print(f"  ❌ 活动{campaign_id}广告位采集失败：{str(e)}")
        return []


def assemble_spot_data(all_spots):
    """组装广告位数据"""
    etl_date = get_etl_date()
    data = []
    field_mapping = [
        "campaign_id", "CAGUID", "GUID", "adposition_type", "area_size",
        "channel_name", "customize", "description", "landing_page", "market",
        "placement_name", "publisher_id", "publisher_name", "report_metrics",
        "spot_id", "spot_id_str", "vending_model"
    ]

    for spot in all_spots:
        row = [safe_str(spot.get(field)) for field in field_mapping]
        row.append(json.dumps(spot.get("spot_plan", []), ensure_ascii=False))
        row.append(json.dumps(spot.get("tracking_tags", []), ensure_ascii=False))
        row.append(safe_str(spot.get("pre_parse_raw_text")))
        row.append(etl_date)
        data.append(row)
    return data


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针广告位表采集任务启动")
    print("=" * 80)

    try:
        # 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 获取活动ID
        campaign_ids = get_campaign_ids(token)

        # 批量采集广告位
        all_spots = []
        for i in range(0, len(campaign_ids), CONFIG["api"]["campaign_batch_size"]):
            batch_ids = campaign_ids[i:i + CONFIG["api"]["campaign_batch_size"]]
            print(f"\n🔹 采集批次{i // CONFIG['api']['campaign_batch_size'] + 1} | 活动数：{len(batch_ids)}")
            for camp_id in batch_ids:
                spots = collect_spot_data(token, camp_id)
                all_spots.extend(spots)
                time.sleep(CONFIG["api"]["interval"])

        # 组装+写入
        data = assemble_spot_data(all_spots)
        partition_dt = datetime.now().strftime("%Y%m%d")
        write_odps_data(odps, data, partition_dt)

        print("\n" + "=" * 80)
        print("✅ 广告位表采集任务完成！")
        print(f"📊 累计采集广告位：{len(all_spots)}条")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()