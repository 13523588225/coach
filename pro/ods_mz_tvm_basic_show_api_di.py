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

# 2. ODPS配置（DataWorks自动鉴权，无需access_id/access_key）
ODPS_PROJECT = ODPS().project  # 自动获取当前DataWorks项目名
TARGET_TABLE = "ods_mz_tvm_basic_show_api_di"  # 唯一目标表

# 3. 核心日期配置（按此范围生成每日分区）
START_DT = '20260101'  # 起始日期（8位，yyyyMMdd）
END_DT = '20260316'  # 结束日期（8位，yyyyMMdd）

# 4. 接口固定参数（必传+遍历）
REPORT_PARAMS = {
    "metrics": "all",  # 必传：获取所有指标
    "by_position": "spot",  # 必传：按广告位维度
    "by_region_list": ["level0", "level1", "level2"]  # 遍历地区维度
}


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
    """按START_DT和END_DT生成每日日期列表（yyyyMMdd），每个日期对应一个分区"""
    dates = []
    try:
        current_dt = datetime.strptime(start_dt, "%Y%m%d")
        end_date_obj = datetime.strptime(end_dt, "%Y%m%d")
        while current_dt <= end_date_obj:
            dates.append(current_dt.strftime("%Y%m%d"))
            current_dt += timedelta(days=1)
    except ValueError as e:
        raise Exception(f"日期范围生成失败：{str(e)}")
    return dates


def is_date_in_campaign_valid(check_date: str, camp_start: str, camp_end: str) -> bool:
    """校验日期是否在活动有效期内（check_date:8位；camp_start/camp_end:10位）"""
    if not all([check_date, camp_start, camp_end]):
        print(f"⚠️ 校验失败：日期/活动有效期为空（检查日期：{check_date}）")
        return False
    try:
        check_dt_obj = datetime.strptime(check_date, "%Y%m%d")
        camp_start_obj = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_end_obj = datetime.strptime(camp_end, "%Y-%m-%d")
        is_valid = camp_start_obj <= check_dt_obj <= camp_end_obj
        if not is_valid:
            print(f"⏩ 日期{check_date}不在活动有效期[{camp_start}~{camp_end}]内，跳过")
        return is_valid
    except ValueError as e:
        print(f"⚠️ 日期格式错误（检查日期：{check_date}）：{str(e)}")
        return False


def to_string(value) -> str:
    """统一空值/None处理，强制转换为字符串"""
    if value is None or value == "" or value == "null":
        return ""
    return str(value)


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取秒针接口鉴权Token"""
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
            raise Exception("Token返回为空")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表（仅保留ID和有效期，用于过滤）"""
    try:
        resp = requests.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])
        valid_campaigns = []
        for c in campaigns:
            if not isinstance(c, dict) or not c.get("campaign_id"):
                continue
            valid_campaigns.append({
                "campaign_id": to_string(c.get("campaign_id")),
                "camp_start_date": to_string(c.get("start_time")),
                "camp_end_date": to_string(c.get("end_time"))
            })
        print(f"✅ 采集到有效活动列表：共{len(valid_campaigns)}个")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")


def parse_report_data(token: str, campaign_id: str, report_date: str, by_region: str) -> List[Dict]:
    """
    调用日报接口并解析返回数据（核心适配真实返回结构）
    :param token: 秒针Token
    :param campaign_id: 活动ID
    :param report_date: 报表日期（10位，yyyy-MM-dd）
    :param by_region: 地区维度（level0/level1/level2）
    :return: 解析后的多region数据列表，失败返回空列表
    """
    # 初始化返回结果
    parse_result = []
    try:
        # 构造完整请求参数（含必传+遍历参数）
        request_params = {
            "campaign_id": campaign_id,
            "date": report_date,
            "metrics": REPORT_PARAMS["metrics"],
            "by_position": REPORT_PARAMS["by_position"],
            "by_region": by_region
        }
        # 调用接口
        resp = requests.get(
            f"{API_CONFIG['report_basic_url']}?access_token={token}",
            params=request_params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        # 校验接口返回状态
        if raw_data.get("error_code") != 0:
            print(f"⚠️ 接口返回错误：{raw_data.get('error_message')}")
            return parse_result
        result = raw_data.get("result", {})
        # 校验核心字段是否存在
        if not all([result.get("date"), result.get("campaignId"), result.get("items")]):
            print(f"⚠️ 接口返回核心字段缺失：{json.dumps(result, ensure_ascii=False)[:200]}...")
            return parse_result

        # 解析items数组（核心：遍历每个region的指标数据）
        items = result.get("items", [])
        etl_date = get_etl_time().split(" ")[0]  # ETL日期（yyyy-MM-dd）
        for item in items:
            if not isinstance(item, dict):
                continue
            # 解析attributes层（受众、地区ID等）
            attributes = item.get("attributes", {})
            # 解析metrics层（处理metrics为null的情况）
            metrics = item.get("metrics", {}) if item.get("metrics") is not None else {}

            # 构造单条数据（严格匹配接口返回字段，空值自动填充）
            single_data = {
                # 顶层公共字段
                "campaign_id": to_string(result.get("campaignId")),
                "report_date": to_string(result.get("date")),
                "version": to_string(result.get("version")),
                "by_region": to_string(by_region),  # 记录当前遍历的地区维度
                # attributes层字段
                "audience": to_string(attributes.get("audience")),
                "universe": to_string(attributes.get("universe")),
                "region_id": to_string(attributes.get("region_id")),
                # metrics层核心指标（接口返回的所有指标）
                "clk_acc": to_string(metrics.get("clk_acc")),
                "clk_avg_day": to_string(metrics.get("clk_avg_day")),
                "clk_day": to_string(metrics.get("clk_day")),
                "imp_acc": to_string(metrics.get("imp_acc")),
                "imp_avg_day": to_string(metrics.get("imp_avg_day")),
                "imp_day": to_string(metrics.get("imp_day")),
                "ucl_acc": to_string(metrics.get("ucl_acc")),
                "ucl_avg_day": to_string(metrics.get("ucl_avg_day")),
                "ucl_day": to_string(metrics.get("ucl_day")),
                "uim_acc": to_string(metrics.get("uim_acc")),
                "uim_avg_day": to_string(metrics.get("uim_avg_day")),
                "uim_day": to_string(metrics.get("uim_day")),
                # ETL相关字段
                "pre_parse_raw_text": to_string(json.dumps(item, ensure_ascii=False)),  # 单条item原始数据
                "etl_date": to_string(etl_date)
            }
            parse_result.append(single_data)

        print(f"🔍 解析完成：campaign_id={campaign_id} | by_region={by_region} | 解析出{len(parse_result)}条region数据")
        return parse_result
    except Exception as e:
        print(f"⚠️ 采集解析失败：campaign_id={campaign_id} | by_region={by_region} | 错误：{str(e)}")
        return parse_result


# ===================== ODPS写入核心函数（原逻辑保留）=====================
def write_to_odps_partition(table_name: str, data: List[List], dt_partition: str):
    """
    按每日分区写入ODPS（清空分区+分批写入，DataWorks自动鉴权）
    :param table_name: 目标表名
    :param data: 待写入数据（二维列表，字段顺序与表一致）
    :param dt_partition: 分区值（8位，yyyyMMdd）
    """
    if not data:
        print(f"⚠️ {table_name} 分区{dt_partition}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt_partition}'"

    try:
        # 清空已有分区，防止数据重复
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
            print(f"🔄 分区{dt_partition} - 写入批次 {i // batch_size + 1}：{len(batch_data)}条")

        print(f"✅ 分区写入完成：{table_name} | 分区{dt_partition} | 总条数{total_count}")
    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")
    except Exception as e:
        raise Exception(f"分区写入异常：{str(e)}")


# ===================== 主流程（整合所有逻辑）=====================
def main():
    """
    核心执行流程：
    1. 按START_DT/END_DT生成每日分区
    2. 校验日期是否在活动有效期内
    3. 遍历by_region[level0/level1/level2]调用接口
    4. 解析接口返回的items数组（多region数据）
    5. 组装数据写入对应dt分区
    """
    try:
        # 任务初始化日志
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"分区范围：{START_DT} ~ {END_DT} | 目标表：{TARGET_TABLE}")
        print(f"接口参数：metrics={REPORT_PARAMS['metrics']} | by_position={REPORT_PARAMS['by_position']}")
        print(f"遍历维度：{REPORT_PARAMS['by_region_list']}")

        # 1. 获取秒针Token
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 2. 获取活动列表（仅用于过滤）
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 3. 生成每日分区日期列表
        daily_partition_dates = get_date_range_by_start_end(START_DT, END_DT)
        print(f"✅ 生成每日分区：共{len(daily_partition_dates)}天")

        # 4. 按每日分区处理数据
        for daily_dt in daily_partition_dates:
            print(f"\n========== 处理分区日期：{daily_dt} ==========")
            daily_write_data = []  # 存储当前分区所有待写入数据

            # 遍历每个活动
            for campaign in campaign_list:
                camp_id = campaign["campaign_id"]
                camp_start = campaign["camp_start_date"]
                camp_end = campaign["camp_end_date"]

                # 核心校验：日期是否在活动有效期内，无效则跳过
                if not is_date_in_campaign_valid(daily_dt, camp_start, camp_end):
                    continue

                # 转换日期格式（8位→10位），供接口调用
                report_date_10bit = date_convert(daily_dt, "10位")
                if not report_date_10bit:
                    print(f"⚠️ 日期{daily_dt}格式转换失败，跳过活动{camp_id}")
                    continue

                # 遍历by_region三个维度：level0/level1/level2
                for by_region in REPORT_PARAMS["by_region_list"]:
                    # 调用接口并解析数据（返回多region数据列表）
                    region_data_list = parse_report_data(token, camp_id, report_date_10bit, by_region)
                    if not region_data_list:
                        time.sleep(API_CONFIG["request_interval"])
                        continue

                    # 组装ODPS写入数据（二维列表，字段顺序与表严格一致）
                    for single_data in region_data_list:
                        write_row = [
                            # 顶层字段
                            single_data["campaign_id"],
                            single_data["report_date"],
                            single_data["version"],
                            single_data["by_region"],
                            # attributes层字段
                            single_data["audience"],
                            single_data["universe"],
                            single_data["region_id"],
                            # metrics层指标（按接口返回顺序）
                            single_data["clk_acc"],
                            single_data["clk_avg_day"],
                            single_data["clk_day"],
                            single_data["imp_acc"],
                            single_data["imp_avg_day"],
                            single_data["imp_day"],
                            single_data["ucl_acc"],
                            single_data["ucl_avg_day"],
                            single_data["ucl_day"],
                            single_data["uim_acc"],
                            single_data["uim_avg_day"],
                            single_data["uim_day"],
                            # ETL字段
                            single_data["pre_parse_raw_text"],
                            single_data["etl_date"],
                            # 分区字段（dt，8位）
                            daily_dt
                        ]
                        daily_write_data.append(write_row)

                    # 接口限流：每个维度调用后休眠
                    time.sleep(API_CONFIG["request_interval"])

            # 5. 写入当前日期的ODPS分区
            if daily_write_data:
                write_to_odps_partition(TARGET_TABLE, daily_write_data, daily_dt)
            else:
                print(f"⚠️ 分区{daily_dt}无有效数据，跳过写入")

        # 任务结束统计
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📊 任务总结：")
        print(f"   - 处理分区数：{len(daily_partition_dates)}个（{START_DT}~{END_DT}）")
        print(f"   - 遍历地区维度：{REPORT_PARAMS['by_region_list']}")
        print(f"   - 核心规则：仅活动有效期内的日期执行采集解析")

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise  # 抛出异常，触发DataWorks任务失败


if __name__ == "__main__":
    main()