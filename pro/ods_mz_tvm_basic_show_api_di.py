# -*- coding: utf-8 -*-
import requests
import json
import time
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from odps import ODPS, errors

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {"username": "Coach_api", "password": "Coachapi2026"},
    "timeout": 30,
    "request_interval": 0.2  # 接口调用间隔，避免限流
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project  # 自动获取当前DataWorks项目名
TARGET_TABLE = "ods_mz_tvm_basic_show_api_di"  # 唯一目标表

# 3. 核心日期配置（按此范围生成每日分区）
START_DT = '20260101'  # 起始日期（8位，yyyyMMdd）
END_DT = '20260316'  # 结束日期（8位，yyyyMMdd）


# ===================== 核心工具函数 =====================
def get_etl_time() -> str:
    """获取当前ETL时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_convert(date_str: str, to_format: str) -> str:
    """日期格式转换：yyyyMMdd <-> yyyy-MM-dd"""
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def get_date_range_by_start_end(start_dt: str, end_dt: str) -> List[str]:
    """
    按START_DT和END_DT生成每日日期列表（yyyyMMdd）
    核心：每个日期对应一个分区
    """
    dates = []
    try:
        current_dt = datetime.strptime(start_dt, "%Y%m%d")
        end_date_obj = datetime.strptime(end_dt, "%Y%m%d")

        # 生成[start_dt, end_dt]范围内的所有日期（含首尾）
        while current_dt <= end_date_obj:
            dates.append(current_dt.strftime("%Y%m%d"))
            current_dt += timedelta(days=1)
    except ValueError as e:
        raise Exception(f"日期范围生成失败：{str(e)}")
    return dates


def is_date_in_campaign_valid(check_date: str, camp_start: str, camp_end: str) -> bool:
    """
    严格校验：检查日期是否在活动的start_date和end_date范围内
    :param check_date: 待检查日期（8位，yyyyMMdd）
    :param camp_start: 活动开始日期（10位，yyyy-MM-dd）
    :param camp_end: 活动结束日期（10位，yyyy-MM-dd）
    :return: 符合返回True，否则False
    """
    # 空值直接返回False
    if not all([check_date, camp_start, camp_end]):
        print(f"⚠️ 校验失败：日期/活动有效期为空（检查日期：{check_date}）")
        return False

    try:
        # 转换为datetime对象进行比较
        check_dt_obj = datetime.strptime(check_date, "%Y%m%d")
        camp_start_obj = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_end_obj = datetime.strptime(camp_end, "%Y-%m-%d")

        # 核心判定：检查日期 >= 活动开始 且 <= 活动结束
        is_valid = camp_start_obj <= check_dt_obj <= camp_end_obj
        if not is_valid:
            print(f"⏩ 日期{check_date}不在活动有效期[{camp_start}~{camp_end}]内，跳过")
        return is_valid
    except ValueError as e:
        print(f"⚠️ 日期格式错误（检查日期：{check_date}，活动开始：{camp_start}，活动结束：{camp_end}）：{str(e)}")
        return False


def to_string(value) -> str:
    """统一空值处理，强制转换为字符串"""
    if value is None or value == "" or value == "null":
        return ""
    return str(value)


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取秒针接口Token"""
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
            raise Exception(f"Token获取失败：{token_data.get('error_message')}")

        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token返回为空")

        return to_string(access_token)
    except Exception as e:
        raise Exception(f"秒针Token获取异常：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表（仅保留有效期校验所需字段）"""
    try:
        resp = requests.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])

        # 仅保留核心字段：活动ID、开始日期、结束日期
        valid_campaigns = []
        for c in campaigns:
            if not isinstance(c, dict) or not c.get("campaign_id"):
                continue
            valid_campaigns.append({
                "campaign_id": to_string(c.get("campaign_id")),
                "camp_start_date": to_string(c.get("start_time")),  # 活动开始（10位，yyyy-MM-dd）
                "camp_end_date": to_string(c.get("end_time"))  # 活动结束（10位，yyyy-MM-dd）
            })

        print(f"✅ 采集到有效活动列表：共{len(valid_campaigns)}个（过滤空ID活动）")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"活动列表采集异常：{str(e)}")


def get_daily_report_data(token: str, campaign_id: str, report_date: str) -> Optional[Dict]:
    """
    调用秒针日报接口，仅当日期在活动有效期内才采集
    :param token: 秒针Token
    :param campaign_id: 活动ID
    :param report_date: 报表日期（10位，yyyy-MM-dd）
    :return: 日报数据字典，失败返回None
    """
    try:
        resp = requests.get(
            f"{API_CONFIG['report_basic_url']}?access_token={token}",
            params={"campaign_id": campaign_id, "date": report_date},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        # 构造符合表结构的日报数据
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


# ===================== ODPS写入核心函数 =====================
def write_to_odps_partition(table_name: str, data: List[List], dt_partition: str):
    """
    按每日分区写入ODPS（每个dt对应一个独立分区）
    :param table_name: 目标表名
    :param data: 待写入数据（二维列表）
    :param dt_partition: 分区值（8位，yyyyMMdd）
    """
    if not data:
        print(f"⚠️ {table_name} 分区{dt_partition}无数据可写入，跳过")
        return

    # 初始化ODPS客户端（DataWorks自动鉴权）
    o = ODPS(project=ODPS_PROJECT)

    # 校验表是否存在
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt_partition}'"  # 单分区：dt

    try:
        # 清空当前分区（防止重复写入）
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 已清空分区：{table_name}.{partition_spec}")

        # 分批写入（每1000条一批，避免超时）
        batch_size = 1000
        total_count = len(data)
        for i in range(0, total_count, batch_size):
            batch_data = data[i:i + batch_size]
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)
            print(f"🔄 分区{dt_partition} - 已写入批次 {i // batch_size + 1}：{len(batch_data)} 条")

        print(f"✅ 分区写入完成：{table_name} | 分区{dt_partition} | 总条数{total_count}")

    except errors.ODPSError as e:
        raise Exception(f"ODPS分区{dt_partition}写入失败：{str(e)}")
    except Exception as e:
        raise Exception(f"分区{dt_partition}写入异常：{str(e)}")


# ===================== 主流程 =====================
def main():
    """
    核心流程：
    1. 按START_DT和END_DT生成每日分区
    2. 校验每个日期是否在活动的start_date和end_date内
    3. 仅对符合条件的日期执行采集+写入对应dt分区
    """
    try:
        # 1. 任务初始化
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"分区生成范围：{START_DT} ~ {END_DT}")
        print(f"目标表：{TARGET_TABLE}（单分区dt，每日一个分区）")

        # 2. 获取秒针Token
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 3. 获取活动列表（仅保留ID和有效期）
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 4. 生成[START_DT, END_DT]范围内的所有日期（每日一个分区）
        daily_partition_dates = get_date_range_by_start_end(START_DT, END_DT)
        print(f"✅ 生成每日分区日期：共{len(daily_partition_dates)}天（{START_DT}~{END_DT}）")

        # 5. 按每日分区处理数据
        for daily_dt in daily_partition_dates:
            print(f"\n========== 处理分区日期：{daily_dt} ==========")
            daily_report_data = []  # 存储当前分区的所有数据

            # 遍历每个活动，校验日期是否在活动有效期内
            for campaign in campaign_list:
                camp_id = campaign["campaign_id"]
                camp_start = campaign["camp_start_date"]
                camp_end = campaign["camp_end_date"]

                # 核心判定：仅当日期在活动有效期内才采集
                if is_date_in_campaign_valid(daily_dt, camp_start, camp_end):
                    # 转换日期格式（8位→10位）供接口调用
                    report_date_10bit = date_convert(daily_dt, "10位")
                    if not report_date_10bit:
                        print(f"⚠️ 日期{daily_dt}格式转换失败，跳过活动{camp_id}")
                        continue

                    # 调用接口采集日报数据
                    report_data = get_daily_report_data(token, camp_id, report_date_10bit)
                    if report_data:
                        # 组装写入数据（最后一列是dt分区字段）
                        report_row = [
                            report_data["campaign_id"],
                            report_data["start_date"],
                            report_data["end_date"],
                            report_data["date"],
                            report_data["version"],
                            report_data["platform"],
                            report_data["total_spot_num"],
                            report_data["audience"],
                            report_data["target_id"],
                            report_data["publisher_id"],
                            report_data["spot_id"],
                            report_data["keyword_id"],
                            report_data["region_id"],
                            report_data["universe"],
                            report_data["imp_acc"],
                            report_data["clk_acc"],
                            report_data["uim_acc"],
                            report_data["ucl_acc"],
                            report_data["imp_day"],
                            report_data["clk_day"],
                            report_data["uim_day"],
                            report_data["ucl_day"],
                            report_data["imp_avg_day"],
                            report_data["clk_avg_day"],
                            report_data["uim_avg_day"],
                            report_data["ucl_avg_day"],
                            report_data["imp_acc_h00"],
                            report_data["imp_acc_h23"],
                            report_data["clk_acc_h00"],
                            report_data["clk_acc_h23"],
                            report_data["pre_parse_raw_text"],
                            report_data["etl_date"],
                            daily_dt  # 分区字段：当前处理的每日日期
                        ]
                        daily_report_data.append(report_row)

                    # 接口限流
                    time.sleep(API_CONFIG["request_interval"])

            # 6. 写入当前日期的分区数据
            if daily_report_data:
                write_to_odps_partition(TARGET_TABLE, daily_report_data, daily_dt)
            else:
                print(f"⚠️ 分区{daily_dt}无有效数据，跳过写入")

        # 7. 任务结束
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📊 任务总结：")
        print(f"   - 分区生成范围：{START_DT} ~ {END_DT}（共{len(daily_partition_dates)}个分区）")
        print(f"   - 目标表：{TARGET_TABLE}（单分区dt，每日一个分区）")
        print(f"   - 核心规则：仅日期在活动start_date/end_date内才执行采集写入")

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise  # 抛出异常，触发DataWorks任务失败


if __name__ == "__main__":
    main()