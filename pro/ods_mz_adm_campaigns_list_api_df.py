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
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 30,
        "interval": 0.2
    },
    "table_name": "ods_mz_adm_campaigns_list_api_df",
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


# ===================== 内置工具函数 =====================
def safe_str(val):
    """安全转换字符串，空值返回空字符串（适配ODPS）"""
    if val is None or val == "" or str(val).lower() in ("null", "undefined"):
        return ""
    return str(val)


def get_etl_datetime():
    """获取数据落地时间"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_miaozhen_token():
    """获取秒针Token（适配Bearer Token方式）"""
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
            raise Exception(f"Token为空，响应：{json.dumps(token_data, ensure_ascii=False)}")
        print("✅ Token获取成功")
        return token
    except Exception as e:
        raise Exception(f"Token获取失败：{str(e)}")


def init_odps_client():
    """初始化ODPS客户端"""
    try:
        odps = ODPS(project=ODPS_PROJECT)
        # 性能优化配置
        options.tunnel.use_instance_tunnel = True
        options.read_timeout = 300
        options.connect_timeout = 60
        options.tunnel.limit_instance_tunnel = False
        print(f"✅ ODPS初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def write_odps_data(odps, data, partition_dt):
    """ODPS批量写入（修复空数据判断）"""
    if not data:
        print(f"⚠️ 表{CONFIG['table_name']}无数据可写入")
        return
    try:
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"表{CONFIG['table_name']}不存在")
        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{partition_dt}'"

        # 清空分区
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {CONFIG['table_name']} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 分批写入
        total_batches = (len(data) + CONFIG["batch_size"] - 1) // CONFIG["batch_size"]
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for i in range(0, len(data), CONFIG["batch_size"]):
                batch_data = data[i:i + CONFIG["batch_size"]]
                writer.write(batch_data)
                print(f"🔸 写入批次{i // CONFIG['batch_size'] + 1}/{total_batches} | 条数：{len(batch_data)}")
        print(f"✅ 表{CONFIG['table_name']}写入完成 | 总条数：{len(data)}")
    except Exception as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ===================== 核心业务逻辑（修复数组解析） =====================
def collect_campaign_data(token):
    """采集活动列表数据（适配接口返回数组格式）"""
    print("🔍 采集活动列表...")
    try:
        # 修复1：Token放在请求头（标准Bearer方式），而非URL参数
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.get(
            CONFIG["api"]["campaign_url"],
            headers=headers,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        pre_parse_raw_text = resp.text
        raw_data = resp.json()

        # 修复2：适配接口返回数组格式，兼容字典格式（兜底）
        if isinstance(raw_data, list):
            campaigns = raw_data  # 接口返回数组直接使用
        elif isinstance(raw_data, dict):
            campaigns = raw_data.get("result", {}).get("campaigns", [])
        else:
            raise Exception(f"接口返回格式异常，非数组/字典：{type(raw_data)}")

        # 过滤有效数据（必须是字典且包含campaign_id）
        valid_camps = []
        for idx, camp in enumerate(campaigns):
            if isinstance(camp, dict) and camp.get("campaign_id"):
                # 为每条数据添加原始文本（溯源）
                camp["pre_parse_raw_text"] = pre_parse_raw_text
                valid_camps.append(camp)
            else:
                print(f"⚠️ 第{idx + 1}条数据无效（无campaign_id/非字典），跳过")

        print(f"✅ 采集到{len(valid_camps)}个有效活动（总计返回{len(campaigns)}条）")
        return valid_camps
    except Exception as e:
        raise Exception(f"活动采集失败：{str(e)}")


def assemble_campaign_data(campaigns):
    """组装活动表数据（字段适配，空值处理）"""
    etl_datetime = get_etl_datetime()
    data = []
    # 字段映射（根据实际接口返回字段调整，示例为通用字段）
    field_mapping = [
        "campaign_id", "start_date", "end_date", "advertiser_name",
        "agency_name", "brand_name", "calculation_type", "campaign_name",
        "campaign_type", "creator_name", "description", "linked_iplib",
        "linked_panels", "linked_siteids", "slot_type", "pre_parse_raw_text"
    ]
    for camp in campaigns:
        # 安全获取每个字段，避免KeyError
        row = [safe_str(camp.get(field, "")) for field in field_mapping]
        row.append(etl_datetime)  # 追加落地时间
        data.append(row)
    return data


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针活动表采集任务启动")
    print("=" * 80)
    try:
        # 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 采集+组装数据
        campaigns = collect_campaign_data(token)
        if not campaigns:
            print("⚠️ 未采集到有效活动数据，任务终止")
            return
        data = assemble_campaign_data(campaigns)

        # 写入ODPS（使用当天日期作为分区）
        write_odps_data(odps, data, '20260318')

        print("\n" + "=" * 80)
        print("✅ 活动表采集任务完成！")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()