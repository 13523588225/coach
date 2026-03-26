# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime
import time
import urllib3
from odps import ODPS, options, errors  # 新增导入errors

# ===================== 基础配置（ODPS自动获取） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "list_spots_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_spots",
        "auth": {
            "grant_type": "password",
            "username": "",  # 后续会被动态替换
            "password": "",  # 后续会被动态替换
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 30,
        "interval": 0.1,
        "campaign_batch_size": 20  # 活动批量采集大小
    },
    "table_name": "ods_mz_adm_list_spots_api_df",  # MaxCompute目标表名
    "batch_size": 1000  # ODPS批量写入大小
}

# 自动获取ODPS项目名
try:
    ODPS_PROJECT = ODPS().project
    if not ODPS_PROJECT:
        raise Exception("ODPS项目名自动获取失败，请检查ODPS客户端配置")
    print(f"✅ 自动获取ODPS项目名：{ODPS_PROJECT}")
except Exception as e:
    raise Exception(f"ODPS项目初始化失败：{str(e)}")


# ===================== 从MaxCompute查询API账号密码 =====================
def get_adm_api_credentials():
    """
    从数仓表 ods_mz_user_api_df 查询ADM接口的账号密码
    SQL: select username,passwords from ods_mz_user_api_df where api_source ='ADM'
    """
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'ADM' limit 1 \
          """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_etl_datetime()}] 🔐 成功从数仓获取ADM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"❌ 查询账号密码失败：{str(e)}")


# ===================== 内置工具函数 =====================
def safe_str(val):
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_miaozhen_token():
    print("🔍 获取秒针Token...")
    try:
        # 从数仓动态获取账号密码
        username, password = get_adm_api_credentials()
        # 构造动态auth参数（替换硬编码值）
        auth_data = CONFIG["api"]["auth"].copy()
        auth_data["username"] = username
        auth_data["password"] = password

        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=auth_data,  # 使用动态获取的账号密码
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
    """正确解析纯数组格式的活动列表，提取campaign_id"""
    print("🔍 获取活动ID列表...")
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        if not isinstance(raw_data, list):
            raise Exception(f"活动列表返回格式异常（非数组），原始数据：{json.dumps(raw_data, ensure_ascii=False)}")

        campaign_ids = []
        for idx, campaign in enumerate(raw_data):
            if not isinstance(campaign, dict):
                print(f"⚠️ 第{idx + 1}条活动数据非字典格式，跳过：{campaign}")
                continue
            camp_id = campaign.get("campaign_id")
            if camp_id and str(camp_id).strip():
                campaign_ids.append(str(camp_id).strip())
            else:
                print(f"⚠️ 第{idx + 1}条活动数据campaign_id为空，跳过")

        campaign_ids = list(set(campaign_ids))
        print(f"✅ 获取到{len(campaign_ids)}个有效活动ID：{campaign_ids}")
        return campaign_ids
    except Exception as e:
        raise Exception(f"活动ID获取失败：{str(e)}")


def init_odps_client():
    """初始化ODPS客户端"""
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
    """写入数据到MaxCompute（清空分区防重复）"""
    if not data:
        print(f"⚠️ 表{CONFIG['table_name']}无数据可写入")
        return

    try:
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"表{CONFIG['table_name']}不存在")

        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{partition_dt}'"

        # 清空当日分区（防止重复数据）
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {CONFIG['table_name']} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 批量写入数据
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
    """适配纯数组格式的广告位数据，正确解析（pre_parse_raw_text仅保留对应字段）"""
    try:
        params = {"access_token": token, "campaign_id": campaign_id}
        # 构建完整请求URL（含参数）
        full_request_url = requests.Request('GET', CONFIG["api"]["list_spots_url"], params=params).prepare().url

        resp = requests.get(
            CONFIG["api"]["list_spots_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        if not isinstance(raw_data, list):
            print(
                f"⚠️ 活动{campaign_id}广告位返回格式异常（非数组），原始数据：{json.dumps(raw_data, ensure_ascii=False)[:100]}...")
            return []

        valid_spots = []
        # 定义需要保留的核心字段（与入库字段对应）
        core_fields = [
            "CAGUID", "GUID", "adposition_type", "area_size", "channel_name",
            "customize", "description", "landing_page", "market", "placement_name",
            "publisher_id", "publisher_name", "report_metrics", "spot_id",
            "spot_id_str", "spot_plan", "tracking_tags", "vending_model"
        ]

        for idx, spot in enumerate(raw_data):
            if not isinstance(spot, dict):
                print(f"  ⚠️ 活动{campaign_id}第{idx + 1}条广告位数据非字典格式，跳过：{spot}")
                continue

            spot_id = spot.get("spot_id")
            if not spot_id or str(spot_id).strip() == "":
                print(f"  ⚠️ 活动{campaign_id}第{idx + 1}条广告位数据spot_id为空，跳过")
                continue

            # 新增：添加完整请求URL字段
            spot["full_request_url"] = full_request_url

            # 核心修改：pre_parse_raw_text仅保留当前广告位的核心字段原始数据
            spot_core_data = {k: spot.get(k) for k in core_fields if k in spot}
            spot["pre_parse_raw_text"] = json.dumps(spot_core_data, ensure_ascii=False)
            # 补充campaign_id关联字段
            spot["campaign_id"] = campaign_id

            valid_spots.append(spot)

        print(f"  ✅ 活动{campaign_id}采集到{len(valid_spots)}个有效广告位")
        return valid_spots
    except Exception as e:
        print(f"  ❌ 活动{campaign_id}广告位采集失败：{str(e)}")
        return []


def assemble_spot_data(all_spots):
    """组装广告位数据（适配ODPS写入格式）"""
    etl_datetime = get_etl_datetime()
    data = []
    # 字段映射（需与MaxCompute表结构完全一致）- 新增full_request_url字段
    field_mapping = [
        "campaign_id", "CAGUID", "GUID", "adposition_type", "area_size",
        "channel_name", "customize", "description", "landing_page", "market",
        "placement_name", "publisher_id", "publisher_name", "report_metrics",
        "spot_id", "spot_id_str", "vending_model", "full_request_url"  # 新增full_request_url
    ]

    for spot in all_spots:
        row = [safe_str(spot.get(field)) for field in field_mapping]
        # 处理数组字段（转为JSON字符串）
        row.append(json.dumps(spot.get("spot_plan", []), ensure_ascii=False))
        row.append(json.dumps(spot.get("tracking_tags", []), ensure_ascii=False))
        # 写入仅保留核心字段的原始数据（pre_parse_raw_text）
        row.append(safe_str(spot.get("pre_parse_raw_text")))
        row.append(etl_datetime)
        data.append(row)
    return data


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针广告位表采集任务启动（写入MaxCompute）")
    print("=" * 80)

    try:
        # 初始化ODPS客户端
        odps = init_odps_client()
        # 获取Token
        token = get_miaozhen_token()
        # 获取活动ID
        campaign_ids = get_campaign_ids(token)

        if not campaign_ids:
            print("⚠️ 未获取到任何有效活动ID，任务终止")
            return

        # 批量采集广告位
        all_spots = []
        for i in range(0, len(campaign_ids), CONFIG["api"]["campaign_batch_size"]):
            batch_ids = campaign_ids[i:i + CONFIG["api"]["campaign_batch_size"]]
            print(f"\n🔹 采集批次{i // CONFIG['api']['campaign_batch_size'] + 1} | 活动数：{len(batch_ids)}")
            for camp_id in batch_ids:
                spots = collect_spot_data(token, camp_id)
                all_spots.extend(spots)
                time.sleep(CONFIG["api"]["interval"])

        # 组装数据 + 写入MaxCompute
        if all_spots:
            data = assemble_spot_data(all_spots)
            write_odps_data(odps, data, args['dt'])
        else:
            print("⚠️ 未采集到任何广告位数据，跳过写入")

        print("\n" + "=" * 80)
        print("✅ 广告位表采集任务完成！")
        print(f"📊 累计采集广告位：{len(all_spots)}条")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()