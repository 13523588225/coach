# -*- coding: utf-8 -*-
import requests
import json
import os
from datetime import datetime, timedelta
import time
import argparse
import urllib3
from typing import Dict, List, Optional, Any
from odps import ODPS, Partition

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {
        "username": "Coach_api",
        "password": "Coachapi2026"
    },
    "timeout": 30,
    "request_interval": 0.2,
    "debug": False
}

# 2. DataWorks/ODPS 配置（自动鉴权）
ODPS_PROJECT = ODPS().project  # 自动获取当前项目名
TABLE_NAMES = {
    "campaign": "ods_mz_tvm_cms_campaigns_list_api_df",
    "report": "ods_mz_tvm_admonitor_basic_show_api_di"
}

# 打印当前项目名
print(f"当前MaxCompute项目：{ODPS_PROJECT}")


# ===================== 命令行参数解析（新增start_date/end_date） =====================
def parse_args():
    """
    解析日期范围参数：start_date/end_date（格式yyyyMMdd）
    校验：1. 日期格式 2. start_date <= end_date
    """
    parser = argparse.ArgumentParser(description='秒针数据采集（按日期范围校验活动有效期）')
    parser.add_argument('--start_date', type=str, required=True, help='采集开始日期（格式：yyyyMMdd）')
    parser.add_argument('--end_date', type=str, required=True, help='采集结束日期（格式：yyyyMMdd）')
    args = parser.parse_args()

    # 日期格式校验
    date_format = "%Y%m%d"
    try:
        start_dt = datetime.strptime(args.start_date, date_format)
        end_dt = datetime.strptime(args.end_date, date_format)
    except ValueError:
        raise ValueError(f"❌ 日期格式错误！请使用{date_format}格式（示例：20260301）")

    # 日期范围校验
    if start_dt > end_dt:
        raise ValueError(f"❌ 开始日期{args.start_date}不能晚于结束日期{args.end_date}")

    # 转换为字符串格式返回
    args.start_date = start_dt.strftime(date_format)
    args.end_date = end_dt.strftime(date_format)

    return args


# ===================== 通用工具函数 =====================
def get_etl_time() -> str:
    """获取当前时间戳（格式：yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ymd8_to_ymd10(date_str: str) -> str:
    """将8位日期（yyyyMMdd）转换为10位日期（yyyy-MM-dd）"""
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def ymd10_to_ymd8(date_str: str) -> str:
    """将10位日期（yyyy-MM-dd）转换为8位日期（yyyyMMdd）"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
    except ValueError:
        return ""


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """
    生成日期范围内的所有日期（8位格式yyyyMMdd）
    :param start_date: 开始日期（yyyyMMdd）
    :param end_date: 结束日期（yyyyMMdd）
    :return: 日期列表（如['20260301','20260302']）
    """
    dates = []
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")

    current_dt = start_dt
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y%m%d"))
        current_dt += timedelta(days=1)

    return dates


def is_date_in_campaign_period(check_date: str, campaign_start: str, campaign_end: str) -> bool:
    """
    校验日期是否在活动有效期内
    :param check_date: 待校验日期（yyyyMMdd）
    :param campaign_start: 活动开始日期（yyyy-MM-dd）
    :param campaign_end: 活动结束日期（yyyy-MM-dd）
    :return: True=在有效期内，False=不在
    """
    # 空值校验
    if not check_date or not campaign_start or not campaign_end:
        return False

    try:
        # 统一转换为datetime对象
        check_dt = datetime.strptime(check_date, "%Y%m%d")
        camp_start_dt = datetime.strptime(campaign_start, "%Y-%m-%d")
        camp_end_dt = datetime.strptime(campaign_end, "%Y-%m-%d")

        # 校验：check_date >= 活动开始 且 check_date <= 活动结束
        return camp_start_dt <= check_dt <= camp_end_dt
    except ValueError:
        # 日期格式错误时返回False
        return False


def safe_convert_type(value, target_type: str) -> Any:
    """安全转换数据类型（兼容空值）"""
    if value is None or value == "" or value == "null":
        if target_type in ("bigint", "int"):
            return None
        else:
            return ""

    try:
        if target_type == "bigint":
            return int(value)
        elif target_type == "int":
            return int(value)
        elif target_type == "string":
            return str(value)
        else:
            return str(value)
    except (ValueError, TypeError):
        if target_type in ("bigint", "int"):
            return None
        else:
            return str(value) if value is not None else ""


# ===================== 秒针接口调用函数 =====================
def get_miaozhen_token() -> str:
    """获取秒针Token"""
    print("\n🔍 请求秒针Token...")
    try:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.post(
            url=API_CONFIG["token_url"],
            data=API_CONFIG["auth"],
            headers=headers,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        token_data = response.json()
        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误 | 码：{token_data.get('error_code')} | 信息：{token_data.get('error_message')}")

        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token为空！")

        print("✅ Token获取成功")
        return access_token
    except Exception as e:
        raise Exception(f"❌ Token获取失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表数据（包含完整有效期信息）"""
    print("\n🔍 采集活动列表数据...")
    try:
        request_url = f"{API_CONFIG['campaign_list_url']}?access_token={token}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        response = requests.get(
            url=request_url,
            headers=headers,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        # 解析数据
        api_raw_text = response.text
        raw_response = json.loads(api_raw_text) if api_raw_text else {}
        campaigns = raw_response.get("result", {}).get("campaigns", [])

        # 格式化活动数据（保留有效期关键信息）
        campaign_list = []
        etl_time = get_etl_time()
        for campaign in campaigns:
            if not isinstance(campaign, dict):
                continue

            campaign_row = {
                "campaign_id": safe_convert_type(campaign.get("campaign_id"), "bigint"),
                "start_time": safe_convert_type(campaign.get("start_time"), "string"),  # 活动开始（yyyy-MM-dd）
                "end_time": safe_convert_type(campaign.get("end_time"), "string"),  # 活动结束（yyyy-MM-dd）
                "order_id": safe_convert_type(campaign.get("order_id"), "string"),
                "scheduling": safe_convert_type(campaign.get("scheduling"), "string"),
                "campaign_name": safe_convert_type(campaign.get("campaign_name"), "string"),
                "description": safe_convert_type(campaign.get("description"), "string"),
                "created_time": safe_convert_type(campaign.get("created_time"), "string"),
                "advertiser": safe_convert_type(campaign.get("advertiser"), "string"),
                "agency": safe_convert_type(campaign.get("agency"), "string"),
                "brand": safe_convert_type(campaign.get("brand"), "string"),
                "status": safe_convert_type(campaign.get("status"), "string"),
                "verify_version": safe_convert_type(campaign.get("verify_version"), "int"),
                "total_net_id": safe_convert_type(campaign.get("total_net_id"), "string"),
                "calculate_type": safe_convert_type(campaign.get("calculate_type"), "string"),
                "totalnet_version": safe_convert_type(campaign.get("totalnet_version"), "string"),
                "sivt_region": safe_convert_type(campaign.get("sivt_region"), "string"),
                "target_list": safe_convert_type(campaign.get("target_list"), "string"),
                "order_title": safe_convert_type(campaign.get("order_title"), "string"),
                "pre_parse_raw_text": json.dumps(campaign, ensure_ascii=False),
                "etl_time": etl_time
            }
            campaign_list.append(campaign_row)

        print(f"✅ 活动列表解析完成 | 共{len(campaign_list)}个活动")
        return campaign_list
    except Exception as e:
        raise Exception(f"❌ 活动列表采集失败：{str(e)}")


def get_daily_report_data(token: str, campaign_id: str, report_date: str) -> Optional[Dict]:
    """
    执行日报接口（仅当日期在活动有效期内时调用）
    :param token: 秒针Token
    :param campaign_id: 活动ID
    :param report_date: 日报日期（yyyy-MM-dd）
    :return: 日报数据（None=调用失败）
    """
    try:
        print(f"  📝 执行日报接口 | 活动{campaign_id} | 日期{report_date}")
        request_url = f"{API_CONFIG['report_basic_url']}?access_token={token}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        params = {"campaign_id": campaign_id, "date": report_date}

        response = requests.get(
            url=request_url,
            headers=headers,
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        # 解析日报数据
        api_raw_text = response.text
        raw_report = json.loads(api_raw_text) if api_raw_text else {}

        report_row = {
            "campaign_id": safe_convert_type(campaign_id, "string"),
            "start_date": safe_convert_type(raw_report.get("start_date"), "string"),
            "end_date": safe_convert_type(raw_report.get("end_date"), "string"),
            "date": safe_convert_type(report_date, "string"),
            "version": safe_convert_type(raw_report.get("version"), "string"),
            "platform": safe_convert_type(raw_report.get("platform"), "string"),
            "total_spot_num": safe_convert_type(raw_report.get("total_spot_num"), "string"),
            "audience": safe_convert_type(raw_report.get("audience"), "string"),
            "target_id": safe_convert_type(raw_report.get("target_id"), "string"),
            "publisher_id": safe_convert_type(raw_report.get("publisher_id"), "string"),
            "spot_id": safe_convert_type(raw_report.get("spot_id"), "string"),
            "keyword_id": safe_convert_type(raw_report.get("keyword_id"), "string"),
            "region_id": safe_convert_type(raw_report.get("region_id"), "string"),
            "universe": safe_convert_type(raw_report.get("universe"), "string"),
            "imp_acc": safe_convert_type(raw_report.get("imp_acc"), "string"),
            "clk_acc": safe_convert_type(raw_report.get("clk_acc"), "string"),
            "uim_acc": safe_convert_type(raw_report.get("uim_acc"), "string"),
            "ucl_acc": safe_convert_type(raw_report.get("ucl_acc"), "string"),
            "imp_day": safe_convert_type(raw_report.get("imp_day"), "string"),
            "clk_day": safe_convert_type(raw_report.get("clk_day"), "string"),
            "uim_day": safe_convert_type(raw_report.get("uim_day"), "string"),
            "ucl_day": safe_convert_type(raw_report.get("ucl_day"), "string"),
            "imp_avg_day": safe_convert_type(raw_report.get("imp_avg_day"), "string"),
            "clk_avg_day": safe_convert_type(raw_report.get("clk_avg_day"), "string"),
            "uim_avg_day": safe_convert_type(raw_report.get("uim_avg_day"), "string"),
            "ucl_avg_day": safe_convert_type(raw_report.get("ucl_avg_day"), "string"),
            "imp_acc_h00": safe_convert_type(raw_report.get("imp_acc_h00"), "string"),
            "imp_acc_h23": safe_convert_type(raw_report.get("imp_acc_h23"), "string"),
            "clk_acc_h00": safe_convert_type(raw_report.get("clk_acc_h00"), "string"),
            "clk_acc_h23": safe_convert_type(raw_report.get("clk_acc_h23"), "string"),
            "pre_parse_raw_text": api_raw_text,
            "etl_date": get_etl_time().split(" ")[0],
            "dt": ymd10_to_ymd8(report_date)  # 分区字段（yyyyMMdd）
        }

        return report_row

    except Exception as e:
        print(f"  ❌ 活动{campaign_id} {report_date}日报接口执行失败：{str(e)}")
        return None


# ===================== MaxCompute数据写入函数 =====================
def write_campaign_to_maxcompute(campaign_data: List[Dict]):
    """写入活动列表数据（按活动创建时间分区）"""
    if not campaign_data:
        print(f"⚠️ 无活动数据可写入")
        return

    try:
        o = ODPS(project=ODPS_PROJECT)
        table_name = TABLE_NAMES["campaign"]

        if not o.exist_table(table_name):
            raise Exception(f"数据表 {table_name} 不存在，请先创建")

        # 写入活动数据（分区为当前日期）
        table = o.get_table(table_name)
        write_rows = []
        current_dt = datetime.now().strftime("%Y%m%d")  # 活动列表按采集日期分区
        for row in campaign_data:
            write_row = [
                row.get("campaign_id"),
                row.get("start_time"),
                row.get("end_time"),
                row.get("order_id"),
                row.get("scheduling"),
                row.get("campaign_name"),
                row.get("description"),
                row.get("created_time"),
                row.get("advertiser"),
                row.get("agency"),
                row.get("brand"),
                row.get("status"),
                row.get("verify_version"),
                row.get("total_net_id"),
                row.get("calculate_type"),
                row.get("totalnet_version"),
                row.get("sivt_region"),
                row.get("target_list"),
                row.get("order_title"),
                row.get("pre_parse_raw_text"),
                row.get("etl_time"),
                current_dt  # 分区字段
            ]
            write_rows.append(write_row)

        with table.open_writer(partition=Partition(table, (current_dt,)), create_partition=True) as writer:
            writer.write(write_rows)

        print(f"✅ 活动列表写入成功 | 表：{table_name} | 分区：{current_dt} | 条数：{len(write_rows)}")
    except Exception as e:
        raise Exception(f"❌ 活动列表写入失败：{str(e)}")


def write_report_to_maxcompute(report_data: List[Dict]):
    """写入日报数据（按日报日期分区）"""
    if not report_data:
        print(f"⚠️ 无日报数据可写入")
        return

    try:
        o = ODPS(project=ODPS_PROJECT)
        table_name = TABLE_NAMES["report"]

        if not o.exist_table(table_name):
            raise Exception(f"数据表 {table_name} 不存在，请先创建")

        # 按分区批量写入日报数据
        report_by_dt = {}
        for row in report_data:
            dt = row.get("dt")
            if dt not in report_by_dt:
                report_by_dt[dt] = []
            # 移除临时dt字段，按表结构组装
            write_row = [
                row.get("campaign_id"),
                row.get("start_date"),
                row.get("end_date"),
                row.get("date"),
                row.get("version"),
                row.get("platform"),
                row.get("total_spot_num"),
                row.get("audience"),
                row.get("target_id"),
                row.get("publisher_id"),
                row.get("spot_id"),
                row.get("keyword_id"),
                row.get("region_id"),
                row.get("universe"),
                row.get("imp_acc"),
                row.get("clk_acc"),
                row.get("uim_acc"),
                row.get("ucl_acc"),
                row.get("imp_day"),
                row.get("clk_day"),
                row.get("uim_day"),
                row.get("ucl_day"),
                row.get("imp_avg_day"),
                row.get("clk_avg_day"),
                row.get("uim_avg_day"),
                row.get("ucl_avg_day"),
                row.get("imp_acc_h00"),
                row.get("imp_acc_h23"),
                row.get("clk_acc_h00"),
                row.get("clk_acc_h23"),
                row.get("pre_parse_raw_text"),
                row.get("etl_date"),
                dt  # 分区字段
            ]
            report_by_dt[dt].append(write_row)

        # 按分区写入
        table = o.get_table(table_name)
        for dt, rows in report_by_dt.items():
            with table.open_writer(partition=Partition(table, (dt,)), create_partition=True) as writer:
                writer.write(rows)
            print(f"✅ 日报数据写入成功 | 表：{table_name} | 分区：{dt} | 条数：{len(rows)}")

    except Exception as e:
        raise Exception(f"❌ 日报数据写入失败：{str(e)}")


# ===================== 主执行流程（核心逻辑） =====================
def main():
    try:
        # 1. 解析参数（start_date/end_date）
        args = parse_args()
        start_date = args.start_date  # yyyyMMdd
        end_date = args.end_date  # yyyyMMdd

        # 打印任务信息
        print("=" * 80)
        print("秒针数据采集（按日期范围校验活动有效期）")
        print(f"执行时间：{get_etl_time()}")
        print(f"采集日期范围：{start_date} ~ {end_date}")
        print("=" * 80)

        # 2. 获取秒针Token
        token = get_miaozhen_token()

        # 3. 采集活动列表数据并写入
        campaign_data = get_campaign_list(token)
        write_campaign_to_maxcompute(campaign_data)

        # 4. 生成日期范围内的所有日期（yyyyMMdd）
        date_range_list = get_date_range(start_date, end_date)
        print(f"\n🔹 待处理日期范围：{date_range_list}（共{len(date_range_list)}天）")

        # 5. 核心逻辑：遍历日期+活动，校验有效期并执行日报接口
        all_report_data = []
        for check_date_8 in date_range_list:
            print(f"\n📅 处理日期：{check_date_8}")
            check_date_10 = ymd8_to_ymd10(check_date_8)  # 转换为yyyy-MM-dd

            # 遍历所有活动，校验有效期
            for campaign in campaign_data:
                camp_id = campaign.get("campaign_id")
                camp_start = campaign.get("start_time")  # yyyy-MM-dd
                camp_end = campaign.get("end_time")  # yyyy-MM-dd

                # 跳过无ID的活动
                if not camp_id:
                    continue

                # 校验：当前日期是否在活动有效期内
                if is_date_in_campaign_period(check_date_8, camp_start, camp_end):
                    # 有效期内：执行日报接口
                    report_row = get_daily_report_data(token, str(camp_id), check_date_10)
                    if report_row:
                        all_report_data.append(report_row)
                else:
                    # 有效期外：跳过
                    print(f"  ⏩ 活动{camp_id} | 日期{check_date_8} | 不在有效期（{camp_start}~{camp_end}）| 跳过日报接口")

                # 接口限流
                time.sleep(API_CONFIG["request_interval"])

        # 6. 写入日报数据
        write_report_to_maxcompute(all_report_data)

        # 任务完成
        print("\n" + "=" * 80)
        print("✅ 全流程执行完成！")
        print(f"📊 统计：")
        print(f"   - 活动列表：{len(campaign_data)} 条")
        print(f"   - 处理日期：{len(date_range_list)} 天")
        print(f"   - 有效日报：{len(all_report_data)} 条")
        print("=" * 80)

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise  # 触发DataWorks任务失败告警


if __name__ == "__main__":
    main()