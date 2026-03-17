# -*- coding: utf-8 -*-
import requests
import json
import time
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from odps import ODPS

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {"username": "Coach_api", "password": "Coachapi2026"},
    "timeout": 30,
    "request_interval": 0.2
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "campaign": "ods_mz_tvm_cms_campaigns_list_api_df",
    "report": "ods_mz_tvm_admonitor_basic_show_api_di"
}

# 3. 固定日期范围（替代参数解析）
start_date = '20260101'
end_date = '20260316'  # 活动列表分区将使用此日期


# ===================== 核心工具函数 =====================
def get_etl_time() -> str:
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


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """生成日期范围内的所有日期（yyyyMMdd）"""
    dates = []
    current_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y%m%d"))
        current_dt += timedelta(days=1)
    return dates


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

        etl_time = get_etl_time()
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
            "etl_time": to_string(etl_time)
        } for c in campaigns if isinstance(c, dict)]
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")


def get_daily_report(token: str, campaign_id: str, report_date: str) -> Optional[Dict]:
    """执行日报接口（仅返回指定字段，所有字段转字符串）"""
    try:
        resp = requests.get(
            f"{API_CONFIG['report_basic_url']}?access_token={token}",
            params={"campaign_id": campaign_id, "date": report_date},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        # 仅返回指定的日报字段
        return {
            "campaign_id": to_string(campaign_id),
            "start_date": to_string(raw_data.get("start_date")),
            "end_date": to_string(raw_data.get("end_date")),
            "date": to_string(report_date),
            "version": to_string(raw_data.get("version")),
            "platform": to_string(raw_data.get("platform")),
            "total_spot_num": to_string(raw_data.get("total_spot_num")),
            "audience": to_string(raw_data.get("audience")),
            "target_id": to_string(raw_data.get("target_id")),
            "publisher_id": to_string(raw_data.get("publisher_id")),
            "spot_id": to_string(raw_data.get("spot_id")),
            "keyword_id": to_string(raw_data.get("keyword_id")),
            "region_id": to_string(raw_data.get("region_id")),
            "universe": to_string(raw_data.get("universe")),
            "imp_acc": to_string(raw_data.get("imp_acc")),
            "clk_acc": to_string(raw_data.get("clk_acc")),
            "uim_acc": to_string(raw_data.get("uim_acc")),
            "ucl_acc": to_string(raw_data.get("ucl_acc")),
            "imp_day": to_string(raw_data.get("imp_day")),
            "clk_day": to_string(raw_data.get("clk_day")),
            "uim_day": to_string(raw_data.get("uim_day")),
            "ucl_day": to_string(raw_data.get("ucl_day")),
            "imp_avg_day": to_string(raw_data.get("imp_avg_day")),
            "clk_avg_day": to_string(raw_data.get("clk_avg_day")),
            "uim_avg_day": to_string(raw_data.get("uim_avg_day")),
            "ucl_avg_day": to_string(raw_data.get("ucl_avg_day")),
            "imp_acc_h00": to_string(raw_data.get("imp_acc_h00")),
            "imp_acc_h23": to_string(raw_data.get("imp_acc_h23")),
            "clk_acc_h00": to_string(raw_data.get("clk_acc_h00")),
            "clk_acc_h23": to_string(raw_data.get("clk_acc_h23")),
            "pre_parse_raw_text": to_string(resp.text),
            "etl_date": to_string(get_etl_time().split(" ")[0])
        }
    except Exception as e:
        print(f"⚠️ 活动{campaign_id} {report_date}日报采集失败：{str(e)}")
        return None


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
        # 1. 打印固定日期范围
        print(f"===== 开始采集：{start_date} ~ {end_date} =====")

        # 2. 获取Token
        token = get_miaozhen_token()
        print(f"✅ Token获取成功")

        # 3. 采集活动列表并写入（分区使用end_date）
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
                c["etl_time"],
                to_string(end_date)  # 活动列表分区字段固定为end_date
            ] for c in campaign_data
        ]
        # 写入活动列表，分区参数传入end_date
        write_to_odps(TABLE_NAMES["campaign"], campaign_write_data, end_date)

        # 4. 遍历日期+活动，采集日报
        report_data = []
        for check_date in get_date_range(start_date, end_date):
            print(f"\n📅 处理日期：{check_date}")
            for campaign in campaign_data:
                camp_id = campaign["campaign_id"]
                if is_date_in_campaign(check_date, campaign["start_time"], campaign["end_time"]):
                    report = get_daily_report(token, camp_id, date_convert(check_date, "10位"))
                    if report:
                        # 严格按指定字段顺序组装
                        report_write_row = [
                            report["campaign_id"],
                            report["start_date"],
                            report["end_date"],
                            report["date"],
                            report["version"],
                            report["platform"],
                            report["total_spot_num"],
                            report["audience"],
                            report["target_id"],
                            report["publisher_id"],
                            report["spot_id"],
                            report["keyword_id"],
                            report["region_id"],
                            report["universe"],
                            report["imp_acc"],
                            report["clk_acc"],
                            report["uim_acc"],
                            report["ucl_acc"],
                            report["imp_day"],
                            report["clk_day"],
                            report["uim_day"],
                            report["ucl_day"],
                            report["imp_avg_day"],
                            report["clk_avg_day"],
                            report["uim_avg_day"],
                            report["ucl_avg_day"],
                            report["imp_acc_h00"],
                            report["imp_acc_h23"],
                            report["clk_acc_h00"],
                            report["clk_acc_h23"],
                            report["pre_parse_raw_text"],
                            report["etl_date"]
                        ]
                        report_data.append(report_write_row)
                    time.sleep(API_CONFIG["request_interval"])
                else:
                    print(f"⏩ 活动{camp_id} 不在有效期，跳过")

        # 5. 写入日报数据（分区字段为日报日期，拼接在最后）
        if report_data:
            report_by_dt = {}
            for row in report_data:
                # 提取日报日期作为分区（从date字段转换为8位）
                dt = date_convert(row[3], "8位")  # row[3]是date字段（yyyy-MM-dd）
                # 分区字段拼接到行最后
                row_with_dt = row + [dt]
                report_by_dt.setdefault(dt, []).append(row_with_dt)

            for dt, rows in report_by_dt.items():
                write_to_odps(TABLE_NAMES["report"], rows, dt)

        print("\n===== 采集完成 =====")
        print(f"📊 统计：活动{len(campaign_data)}个 | 有效日报{len(report_data)}条")

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()