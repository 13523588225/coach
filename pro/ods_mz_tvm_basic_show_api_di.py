# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from odps import ODPS, errors
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {"username": "Coach_api", "password": "Coachapi2026"},
    "timeout": 30,
    "request_interval": 0.01  # 请求间隔0.01秒
}

# 2. ODPS配置
ODPS_PROJECT = ODPS().project
TARGET_TABLE = "coach_marketing_hub_dev.ods_mz_tvm_basic_show_api_di"

# 3. 日期配置
START_DT = '20260301'
END_DT = '20260305'

# 4. 接口固定参数
REPORT_PARAMS = {
    "metrics": "all",
    "by_position": "spot",
    "by_region_list": ["level0", "level1", "level2"]
}

# 5. 并行/批次配置（优化内存）
PARALLEL_CONFIG = {
    "campaign_max_workers": 10,  # 活动解析并行数
    "partition_max_workers": 3,  # 分区写入并行数（根据ODPS资源调整）
    "batch_size": 20000  # 每批次写入2万条
}

# 6. 小时粒度字段列表（h00~h23）
HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]


# ===================== 工具函数 =====================
def get_etl_time() -> str:
    """获取ETL时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_convert(date_str: str, to_format: str) -> str:
    """日期格式转换"""
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def get_date_range_by_start_end(start_dt: str, end_dt: str) -> List[str]:
    """生成每日日期列表"""
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
    """校验日期是否在活动有效期内"""
    if not all([check_date, camp_start, camp_end]):
        return False
    try:
        check_dt_obj = datetime.strptime(check_date, "%Y%m%d")
        camp_start_obj = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_end_obj = datetime.strptime(camp_end, "%Y-%m-%d")
        return camp_start_obj <= check_dt_obj <= camp_end_obj
    except ValueError:
        return False


def to_unified_string(value) -> str:
    """统一转换为字符串（所有字段输出为字符串，空值返回空字符串）"""
    if value is None or value == "" or value == "null":
        return ""
    # 数值型统一转字符串（避免科学计数法）
    if isinstance(value, (int, float)):
        return str(int(value)) if isinstance(value, float) and value.is_integer() else str(value)
    return str(value)


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取鉴权Token"""
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
        return to_unified_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表"""
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
            if isinstance(c, dict) and c.get("campaign_id"):
                valid_campaigns.append({
                    "campaign_id": to_unified_string(c.get("campaign_id")),
                    "camp_start_date": to_unified_string(c.get("start_time")),
                    "camp_end_date": to_unified_string(c.get("end_time"))
                })
        print(f"✅ 采集到有效活动列表：共{len(valid_campaigns)}个")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")


def parse_single_campaign(token: str, campaign: Dict, daily_dt: str) -> List[List[str]]:
    """解析单个活动的单日期数据（所有字段输出为字符串）"""
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]

    # 校验日期有效性
    if not is_date_in_campaign_valid(daily_dt, camp_start, camp_end):
        return campaign_data

    # 转换日期格式
    report_date_10bit = date_convert(daily_dt, "10位")
    if not report_date_10bit:
        return campaign_data

    # 遍历地区维度
    for by_region in REPORT_PARAMS["by_region_list"]:
        try:
            # 构造请求参数
            request_params = {
                "campaign_id": camp_id,
                "date": report_date_10bit,
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

            # 校验返回结果
            if raw_data.get("error_code") != 0:
                time.sleep(API_CONFIG["request_interval"])
                continue
            result = raw_data.get("result", {})
            if not all([result.get("date"), result.get("campaignId"), result.get("items")]):
                time.sleep(API_CONFIG["request_interval"])
                continue

            # 解析items数据
            items = result.get("items", [])
            etl_datetime = get_etl_time()

            for item in items:
                if not isinstance(item, dict):
                    continue
                attributes = item.get("attributes", {})
                metrics = item.get("metrics", {}) if item.get("metrics") is not None else {}

                # 生成当前行的pre_parse_raw_text（仅attributes+metrics）
                current_row_data = {
                    "attributes": attributes,
                    "metrics": metrics
                }
                pre_parse_raw_text = to_unified_string(json.dumps(current_row_data, ensure_ascii=False, indent=None))

                # 组装请求参数字段（全字符串）
                request_fields = [
                    to_unified_string(request_params["campaign_id"]),
                    to_unified_string(request_params["date"]),
                    to_unified_string(request_params["metrics"]),
                    to_unified_string(request_params["by_position"]),
                    to_unified_string(request_params["by_region"])
                ]

                # 组装活动基础字段（全字符串）
                base_fields = [
                    to_unified_string(result.get("campaignId")),
                    to_unified_string(camp_start),
                    to_unified_string(camp_end),
                    to_unified_string(result.get("date")),
                    to_unified_string(result.get("version")),
                    to_unified_string(attributes.get("publisher_id")),
                    to_unified_string(attributes.get("spot_id")),
                    to_unified_string(attributes.get("spot_id_str")),
                    to_unified_string(attributes.get("audience")),
                    to_unified_string(attributes.get("universe")),
                    to_unified_string(attributes.get("region_id")),
                    to_unified_string(metrics.get("imp_acc")),
                    to_unified_string(metrics.get("clk_acc")),
                    to_unified_string(metrics.get("uim_acc")),
                    to_unified_string(metrics.get("ucl_acc")),
                    to_unified_string(metrics.get("imp_day")),
                    to_unified_string(metrics.get("clk_day")),
                    to_unified_string(metrics.get("uim_day")),
                    to_unified_string(metrics.get("ucl_day")),
                    to_unified_string(metrics.get("imp_avg_day")),
                    to_unified_string(metrics.get("clk_avg_day")),
                    to_unified_string(metrics.get("uim_avg_day")),
                    to_unified_string(metrics.get("ucl_avg_day")),
                ]

                # 组装小时粒度曝光指标（全字符串）
                imp_hour_fields = [to_unified_string(metrics.get(f"imp_{hour}")) for hour in HOUR_FIELDS]

                # 组装小时粒度点击指标（全字符串）
                clk_hour_fields = [to_unified_string(metrics.get(f"clk_{hour}")) for hour in HOUR_FIELDS]

                # 组装元数据字段（全字符串）
                meta_fields = [
                    pre_parse_raw_text,
                    to_unified_string(etl_datetime)
                ]

                # 合并所有字段（全字符串列表）
                write_row = request_fields + base_fields + imp_hour_fields + clk_hour_fields + meta_fields
                campaign_data.append(write_row)

            time.sleep(API_CONFIG["request_interval"])
        except Exception:
            time.sleep(API_CONFIG["request_interval"])
            continue

    return campaign_data


def process_single_partition(token: str, campaign_list: List[Dict], daily_dt: str) -> Tuple[str, List[List[str]]]:
    """处理单个分区（解析+数据组装），返回分区日期和数据"""
    print(f"\n🚀 开始处理分区：{daily_dt}")
    partition_data = []

    # 并行解析活动数据
    with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["campaign_max_workers"]) as executor:
        future_to_campaign = {
            executor.submit(parse_single_campaign, token, campaign, daily_dt): campaign
            for campaign in campaign_list
        }

        # 收集结果
        for future in as_completed(future_to_campaign):
            try:
                campaign_data = future.result()
                if campaign_data:
                    partition_data.extend(campaign_data)
            except Exception as e:
                print(f"❌ 分区{daily_dt} - 活动解析失败：{str(e)}")
                continue

    print(f"✅ 分区{daily_dt} - 解析完成，共生成{len(partition_data)}条数据")
    return (daily_dt, partition_data)


# ===================== ODPS写入 =====================
def write_to_odps_partition(table_name: str, data: List[List[str]], dt_partition: str):
    """按分区写入ODPS（所有字段为字符串，记录批次耗时）"""
    if not data:
        print(f"⚠️ 分区{dt_partition}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt_partition}'"
    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size  # 向上取整计算总批次

    try:
        # 清空已有分区
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 分区{dt_partition} - 已清空历史数据")

        # 打印批次规划信息
        print(f"📊 分区{dt_partition} - 总数据量{total_count}条，分{batch_num}批次写入（每批次{batch_size}条）")

        # 分批写入（记录每个批次耗时）
        total_batch_time = 0  # 累计所有批次耗时
        for i in range(batch_num):
            # 记录批次开始时间
            batch_start_time = time.time()

            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_count)
            batch_data = data[start_idx:end_idx]
            batch_actual_count = len(batch_data)

            # 写入当前批次（所有字段为字符串，适配ODPS表）
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)

            # 计算批次耗时（保留2位小数）
            batch_cost_time = round(time.time() - batch_start_time, 2)
            total_batch_time += batch_cost_time

            # 打印当前批次进度+耗时
            print(
                f"🔄 分区{dt_partition} - 批次{i + 1}/{batch_num}：写入{batch_actual_count}条（范围：{start_idx + 1}~{end_idx}），耗时{batch_cost_time}秒")
            gc.collect()

        # 打印分区写入完成总结（含总耗时）
        total_batch_time = round(total_batch_time, 2)
        print(
            f"✅ 分区{dt_partition}写入完成，累计写入{total_count}条，总耗时{total_batch_time}秒，平均每批次{round(total_batch_time / batch_num, 2)}秒")
    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")
    except Exception as e:
        raise Exception(f"分区写入异常：{str(e)}")


# ===================== 主流程 =====================
def main():
    """核心执行流程（分区并行执行插入）"""
    try:
        # 记录任务总开始时间
        task_start_time = time.time()

        # 初始化日志
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"分区范围：{START_DT} ~ {END_DT} | 目标表：{TARGET_TABLE}")
        print(
            f"并行配置：活动解析={PARALLEL_CONFIG['campaign_max_workers']} | 分区写入={PARALLEL_CONFIG['partition_max_workers']} | 批次大小={PARALLEL_CONFIG['batch_size']}")
        print(f"接口配置：请求间隔={API_CONFIG['request_interval']}秒 | 所有字段输出为字符串")

        # 1. 获取Token
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 2. 获取活动列表
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 3. 生成日期列表
        daily_partition_dates = get_date_range_by_start_end(START_DT, END_DT)
        print(f"✅ 生成每日分区：共{len(daily_partition_dates)}天")

        # 4. 并行解析所有分区数据（先解析，再并行写入）
        partition_data_map = {}
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["campaign_max_workers"]) as parse_executor:
            future_to_partition = {
                parse_executor.submit(process_single_partition, token, campaign_list, dt): dt
                for dt in daily_partition_dates
            }

            for future in as_completed(future_to_partition):
                dt = future_to_partition[future]
                try:
                    partition_dt, partition_data = future.result()
                    partition_data_map[partition_dt] = partition_data
                except Exception as e:
                    print(f"❌ 分区{dt}解析失败：{str(e)}")
                    continue

        # 5. 并行写入所有分区数据
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["partition_max_workers"]) as write_executor:
            future_to_write = {
                write_executor.submit(write_to_odps_partition, TARGET_TABLE, data, dt): dt
                for dt, data in partition_data_map.items()
            }

            # 等待所有分区写入完成
            for future in as_completed(future_to_write):
                dt = future_to_write[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ 分区{dt}写入失败：{str(e)}")
                    continue

        # 计算任务总耗时
        task_cost_time = round(time.time() - task_start_time, 2)
        # 任务结束
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📈 任务总耗时：{task_cost_time}秒 | 处理分区数：{len(partition_data_map)}")
    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()