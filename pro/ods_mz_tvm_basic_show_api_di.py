# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
from datetime import datetime
from typing import Dict, List
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
    "request_interval": 0.01
}

# 2. ODPS配置（匹配表结构的库表名）
ODPS_PROJECT = ODPS().project
TARGET_TABLE = "coach_marketing_hub_dev.ods_mz_tvm_basic_show_api_di"  # 完整表名

# 3. 单分区日期配置
DT = '20260301'

# 4. 接口参数（按要求调整）
REPORT_PARAMS = {
    "metrics": "all",
    "by_region": ["level0", "level1", "level2"],
    "by_audience": ["overall", "stable", "target"],
    "platform": ["pc", "pm", "mb"],
    "by_position": ["campaign", "publisher", "spot", "keyword"]
}

# 5. 并行/批次配置
PARALLEL_CONFIG = {
    "max_workers": 10,
    "batch_size": 20000
}

# 6. 小时粒度字段列表（严格对应表结构的h00~h23）
HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]

# ===================== 工具函数（严格匹配字段类型） =====================
def get_etl_time() -> str:
    """获取ETL时间戳（格式：YYYY-MM-DD HH:MM:SS）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def date_convert(date_str: str, to_format: str) -> str:
    """日期格式转换：8位↔10位"""
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""

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

def to_string(value) -> str:
    """空值处理为空字符串（匹配表结构STRING类型）"""
    if value is None or value == "" or value == "null":
        return ""
    return str(value).strip()

def to_bigint(value) -> int:
    """转换为BIGINT类型（匹配表结构，空值/非数值填0）"""
    if value is None or value == "" or value == "null":
        return 0
    try:
        return int(float(value))  # 先转float避免字符串小数问题，再转int
    except (ValueError, TypeError):
        return 0

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
        return to_string(access_token)
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
                    "campaign_id": to_string(c.get("campaign_id")),
                    "camp_start_date": to_string(c.get("start_time")),
                    "camp_end_date": to_string(c.get("end_time"))
                })
        print(f"✅ 采集到有效活动列表：共{len(valid_campaigns)}个")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")

def parse_single_campaign(token: str, campaign: Dict) -> List[List]:
    """解析单个活动数据（严格匹配表结构字段顺序）"""
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]

    # 校验日期有效性
    if not is_date_in_campaign_valid(DT, camp_start, camp_end):
        return campaign_data

    # 转换日期格式（10位报表日期）
    report_date_10bit = date_convert(DT, "10位")
    if not report_date_10bit:
        return campaign_data

    # 遍历所有维度组合
    for by_region in REPORT_PARAMS["by_region"]:
        for by_audience in REPORT_PARAMS["by_audience"]:
            for platform in REPORT_PARAMS["platform"]:
                for by_position in REPORT_PARAMS["by_position"]:
                    try:
                        # 构造请求参数
                        request_params = {
                            "campaign_id": camp_id,
                            "date": report_date_10bit,
                            "metrics": REPORT_PARAMS["metrics"],
                            "by_region": by_region,
                            "by_audience": by_audience,
                            "platform": platform,
                            "by_position": by_position
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

                            # 1. 原始解析文本（JSON字符串）
                            pre_parse_raw_text = to_string(
                                json.dumps({"attributes": attributes, "metrics": metrics}, ensure_ascii=False, indent=None)
                            )

                            # ===================== 严格按表结构顺序组装字段 =====================
                            write_row = [
                                # 1. 请求参数字段（STRING）
                                to_string(request_params["campaign_id"]),          # request_campaign_id
                                to_string(request_params["date"]),                 # request_date
                                to_string(request_params["metrics"]),              # request_metrics
                                to_string(request_params["by_position"]),          # request_by_position
                                to_string(request_params["by_region"]),            # request_by_region

                                # 2. 活动基础字段（混合类型）
                                to_string(result.get("campaignId")),               # campaign_id
                                to_string(camp_start),                             # camp_start_date
                                to_string(camp_end),                               # camp_end_date
                                to_string(result.get("date")),                     # report_date
                                to_bigint(result.get("version")),                  # version (BIGINT)
                                to_string(attributes.get("publisher_id")),         # publisher_id
                                to_string(attributes.get("spot_id")),              # spot_id
                                to_string(attributes.get("spot_id_str")),          # spot_id_str
                                to_string(attributes.get("audience")),             # audience
                                to_string(attributes.get("universe")),             # universe
                                to_string(attributes.get("region_id")),            # region_id

                                # 3. 核心指标（BIGINT）
                                to_bigint(metrics.get("imp_acc")),                 # imp_acc
                                to_bigint(metrics.get("clk_acc")),                 # clk_acc
                                to_bigint(metrics.get("uim_acc")),                 # uim_acc
                                to_bigint(metrics.get("ucl_acc")),                 # ucl_acc
                                to_bigint(metrics.get("imp_day")),                 # imp_day
                                to_bigint(metrics.get("clk_day")),                 # clk_day
                                to_bigint(metrics.get("uim_day")),                 # uim_day
                                to_bigint(metrics.get("ucl_day")),                 # ucl_day
                                to_bigint(metrics.get("imp_avg_day")),             # imp_avg_day
                                to_bigint(metrics.get("clk_avg_day")),             # clk_avg_day
                                to_bigint(metrics.get("uim_avg_day")),             # uim_avg_day
                                to_bigint(metrics.get("ucl_avg_day")),             # ucl_avg_day

                                # 4. 小时粒度曝光指标（BIGINT，h00~h23）
                                *[to_bigint(metrics.get(f"imp_{hour}")) for hour in HOUR_FIELDS],

                                # 5. 小时粒度点击指标（BIGINT，h00~h23）
                                *[to_bigint(metrics.get(f"clk_{hour}")) for hour in HOUR_FIELDS],

                                # 6. 元数据字段（STRING）
                                pre_parse_raw_text,                                # pre_parse_raw_text
                                etl_datetime                                      # etl_datetime
                            ]
                            campaign_data.append(write_row)

                        time.sleep(API_CONFIG["request_interval"])
                    except Exception:
                        time.sleep(API_CONFIG["request_interval"])
                        continue

    return campaign_data

# ===================== ODPS写入（适配分区表结构） =====================
def write_to_odps_partition(table_name: str, data: List[List]):
    """按表结构写入ODPS分区"""
    if not data:
        print(f"⚠️ 分区{DT}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{DT}'"
    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size  # 向上取整

    try:
        # 清空已有分区（避免数据重复）
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 已清空分区：{DT}")

        # 打印批次规划
        print(f"📊 分区{DT} - 总数据量{total_count}条，分{batch_num}批次写入（每批次{batch_size}条）")

        # 分批写入（严格匹配字段顺序和类型）
        total_batch_time = 0
        for i in range(batch_num):
            batch_start_time = time.time()
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_count)
            batch_data = data[start_idx:end_idx]
            batch_actual_count = len(batch_data)

            # 写入当前批次（自动匹配表结构字段顺序）
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)

            # 统计耗时
            batch_cost_time = round(time.time() - batch_start_time, 2)
            total_batch_time += batch_cost_time

            # 打印批次进度
            print(f"🔄 分区{DT} - 批次{i + 1}/{batch_num}：写入{batch_actual_count}条，耗时{batch_cost_time}秒")
            gc.collect()

        # 写入完成总结
        total_batch_time = round(total_batch_time, 2)
        print(f"✅ 分区{DT}写入完成，累计写入{total_count}条，总耗时{total_batch_time}秒，平均每批次{round(total_batch_time / batch_num, 2)}秒")
    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")
    except Exception as e:
        raise Exception(f"分区写入异常：{str(e)}")

# ===================== 主流程 =====================
def main():
    """核心执行流程（适配表结构）"""
    try:
        # 记录总耗时
        task_start_time = time.time()

        # 初始化日志
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"目标分区：{DT} | 目标表：{TARGET_TABLE}")
        print(f"并行配置：max_workers={PARALLEL_CONFIG['max_workers']} | 批次大小={PARALLEL_CONFIG['batch_size']}")

        # 1. 获取Token
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 2. 获取活动列表
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 3. 并行解析单分区数据
        print(f"\n========== 处理分区：{DT} ==========")
        daily_write_data = []

        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["max_workers"]) as executor:
            future_to_campaign = {
                executor.submit(parse_single_campaign, token, campaign): campaign
                for campaign in campaign_list
            }

            # 收集解析结果
            for future in as_completed(future_to_campaign):
                try:
                    campaign_data = future.result()
                    if campaign_data:
                        daily_write_data.extend(campaign_data)
                except Exception as e:
                    print(f"❌ 活动解析失败：{str(e)}")
                    continue

        # 4. 写入ODPS单分区
        if daily_write_data:
            write_to_odps_partition(TARGET_TABLE, daily_write_data)
        else:
            print(f"⚠️ 分区{DT}无有效数据，跳过写入")

        # 5. 任务总结
        task_cost_time = round(time.time() - task_start_time, 2)
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📈 任务总耗时：{task_cost_time}秒")
    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise

if __name__ == "__main__":
    main()