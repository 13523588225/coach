import json
from datetime import datetime
from typing import Dict, List
import requests
import urllib3
from odps import ODPS

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "auth": {"username": "Coach_api", "password": "Coachapi2026"},
    "timeout": 30,
    "request_interval": 0.2
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "campaign": "ods_mz_tvm_campaigns_list_api_df"
}

# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_convert(date_str: str, to_format: str) -> str:
    """日期格式转换：仅支持 yyyyMMdd <-> yyyy-MM-dd"""
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def is_date_in_campaign(check_date: str, camp_start: str, camp_end: str) -> bool:
    """校验日期是否在活动有效期内"""
    if not all([check_date, camp_start, camp_end]):
        return False
    try:
        check_dt = datetime.strptime(check_date, "%Y%m%d")
        return datetime.strptime(camp_start, "%Y-%m-%d") <= check_dt <= datetime.strptime(camp_end, "%Y-%m-%d")
    except ValueError:
        return False


def to_string(value) -> str:
    """强制转换为字符串"""
    if value is None or value == "" or value == "null":
        return ""
    return str(value)


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取秒针Token"""
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
        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误：{token_data.get('error_message')}")
        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token为空")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表（所有字段转字符串）"""
    try:
        resp = requests.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])

        etl_datetime = get_etl_datetime()
        return [{
            "campaign_id": to_string(c.get("campaign_id")),
            "start_time": to_string(c.get("start_time")),
            "end_time": to_string(c.get("end_time")),
            "order_id": to_string(c.get("order_id")),
            "scheduling": to_string(c.get("scheduling")),
            "campaign_name": to_string(c.get("campaign_name")),
            "description": to_string(c.get("description")),
            "created_time": to_string(c.get("created_time")),
            "advertiser": to_string(c.get("advertiser")),
            "agency": to_string(c.get("agency")),
            "brand": to_string(c.get("brand")),
            "status": to_string(c.get("status")),
            "verify_version": to_string(c.get("verify_version")),
            "total_net_id": to_string(c.get("total_net_id")),
            "calculate_type": to_string(c.get("calculate_type")),
            "totalnet_version": to_string(c.get("totalnet_version")),
            "sivt_region": to_string(c.get("sivt_region")),
            "target_list": to_string(c.get("target_list")),
            "order_title": to_string(c.get("order_title")),
            "pre_parse_raw_text": to_string(json.dumps(c, ensure_ascii=False)),
            "etl_datetime": to_string(etl_datetime)
        } for c in campaigns if isinstance(c, dict)]
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")




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
    """
    脚本主执行流程：
    1. 获取秒针Token
    2. 采集活动列表数据
    3. 格式化写入ODPS
    """
    try:

        # 1. 获取秒针Token
        token = get_miaozhen_token()
        print(f"✅ Token获取成功")

        # 2. 采集活动列表数据
        campaign_data = get_campaign_list(token)
        campaign_write_data = [
            [
                c["campaign_id"],
                c["start_time"],
                c["end_time"],
                c["order_id"],
                c["scheduling"],
                c["campaign_name"],
                c["description"],
                c["created_time"],
                c["advertiser"],
                c["agency"],
                c["brand"],
                c["status"],
                c["verify_version"],
                c["total_net_id"],
                c["calculate_type"],
                c["totalnet_version"],
                c["sivt_region"],
                c["target_list"],
                c["order_title"],
                c["pre_parse_raw_text"],
                c["etl_datetime"]
            ] for c in campaign_data
        ]
        # 3. 格式化写入ODPS，分区参数传入dt
        # write_to_odps(TABLE_NAMES["campaign"], campaign_write_data, args['dt'])
        write_to_odps(TABLE_NAMES["campaign"], campaign_write_data, '20260318')

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()
