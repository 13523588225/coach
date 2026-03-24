# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
import traceback
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
    "request_interval": 0.003
}

# 2. ODPS配置
ODPS_PROJECT = ODPS().project
TARGET_TABLE = "coach_marketing_hub_dev.ods_mz_tvm_basic_show_api_di"

# 3. 单分区日期
DT = '20260301'

# 4. 接口维度参数（调整为更安全的组合，避免无效参数）
REPORT_PARAMS = {
    "metrics": "all",
    "by_region": ["level0"],  # 先简化为level0，避免多级区域参数错误
    "by_audience": ["overall"],  # 先简化为overall，避免受众类型错误
    "platform": ["pc"],  # 先简化为pc，避免平台参数错误
    "by_position": ["campaign"]  # 先简化为campaign，避免维度参数错误
}

# 5. 并行/批次配置
PARALLEL_CONFIG = {
    "max_workers": 6,  # 降低并发数，避免接口限流
    "batch_size": 50000
}

# 6. 小时字段
HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]

# ===================== 全局性能优化 =====================
SESSION = requests.Session()
gc.disable()


# ===================== 工具函数 =====================
def get_etl_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_convert(date_str: str, to_format: str) -> str:
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def is_date_in_campaign_valid(check_date: str, camp_start: str, camp_end: str) -> bool:
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
    if value is None or value == "" or value == "null":
        return ""
    return str(value).strip()


def to_bigint(value) -> int:
    if value is None or value == "" or value == "null":
        return 0
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return 0


# ===================== 接口调用 =====================
def get_miaozhen_token() -> str:
    try:
        resp = SESSION.post(
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
    try:
        resp = SESSION.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])
        valid_campaigns = []
        for c in campaigns:
            if isinstance(c, dict) and c.get("campaign_id"):
                # 过滤掉日期不匹配的活动
                camp_start = to_string(c.get("start_time"))
                camp_end = to_string(c.get("end_time"))
                if is_date_in_campaign_valid(DT, camp_start, camp_end):
                    valid_campaigns.append({
                        "campaign_id": to_string(c.get("campaign_id")),
                        "camp_start_date": camp_start,
                        "camp_end_date": camp_end
                    })
        print(f"✅ 采集到有效活动列表（日期匹配）：共{len(valid_campaigns)}个")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")


def parse_single_campaign(token: str, campaign: Dict) -> List[List]:
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]
    report_date_10bit = date_convert(DT, "10位")
    if not report_date_10bit:
        return campaign_data

    for by_region in REPORT_PARAMS["by_region"]:
        for by_audience in REPORT_PARAMS["by_audience"]:
            for platform in REPORT_PARAMS["platform"]:
                for by_position in REPORT_PARAMS["by_position"]:
                    try:
                        # 核心修复：请求参数中加入access_token
                        request_params = {
                            "access_token": token,  # 新增：必传的token参数
                            "campaign_id": camp_id,
                            "date": report_date_10bit,
                            "metrics": REPORT_PARAMS["metrics"],
                            "by_region": by_region,
                            "by_audience": by_audience,
                            "platform": platform,
                            "by_position": by_position
                        }

                        # ========== 全量参数打印（不含token）+ 接口耗时 ==========
                        req_start = time.time()
                        resp = SESSION.get(
                            API_CONFIG["report_basic_url"],
                            params=request_params,
                            timeout=API_CONFIG["timeout"],
                            verify=False
                        )
                        req_cost = round(time.time() - req_start, 4)

                        # 打印请求参数（隐藏token）
                        log_params = request_params.copy()
                        log_params.pop("access_token", None)
                        print(
                            f"📡 接口请求 | campaign={camp_id} | 参数={log_params} | 耗时={req_cost}s")

                        resp.raise_for_status()
                        raw_data = resp.json()

                        # ===================== 无有效数据判定 + 详细日志 =====================
                        if raw_data.get("error_code") != 0:
                            print(f"⚠️  接口返回错误 | campaign={camp_id} | 错误信息：{raw_data.get('error_message')}")
                            time.sleep(API_CONFIG["request_interval"])
                            continue
                        result = raw_data.get("result", {})
                        items = result.get("items", [])
                        if not result or items is None or len(items) == 0:
                            print(f"⚠️  接口无有效数据 | campaign={camp_id} | URL：{resp.url}")
                            time.sleep(API_CONFIG["request_interval"])
                            continue
                        # ======================================================================

                        if not all([result.get("date"), result.get("campaignId"), result.get("items")]):
                            print(f"⚠️  接口返回数据不完整 | campaign={camp_id} | URL：{resp.url}")
                            time.sleep(API_CONFIG["request_interval"])
                            continue

                        etl_datetime = get_etl_time()

                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            attributes = item.get("attributes", {})
                            metrics = item.get("metrics", {}) if item.get("metrics") is not None else {}

                            pre_parse_raw_text = to_string(
                                json.dumps({"attributes": attributes, "metrics": metrics}, ensure_ascii=False,
                                           indent=None)
                            )

                            write_row = [
                                to_string(request_params["campaign_id"]),
                                to_string(request_params["date"]),
                                to_string(request_params["metrics"]),
                                to_string(request_params["by_position"]),
                                to_string(request_params["by_region"]),

                                to_string(result.get("campaignId")),
                                to_string(camp_start),
                                to_string(camp_end),
                                to_string(result.get("date")),
                                to_bigint(result.get("version")),
                                to_string(attributes.get("publisher_id")),
                                to_string(attributes.get("spot_id")),
                                to_string(attributes.get("spot_id_str")),
                                to_string(attributes.get("audience")),
                                to_string(attributes.get("universe")),
                                to_string(attributes.get("region_id")),

                                to_bigint(metrics.get("imp_acc")),
                                to_bigint(metrics.get("clk_acc")),
                                to_bigint(metrics.get("uim_acc")),
                                to_bigint(metrics.get("ucl_acc")),
                                to_bigint(metrics.get("imp_day")),
                                to_bigint(metrics.get("clk_day")),
                                to_bigint(metrics.get("uim_day")),
                                to_bigint(metrics.get("ucl_day")),
                                to_bigint(metrics.get("imp_avg_day")),
                                to_bigint(metrics.get("clk_avg_day")),
                                to_bigint(metrics.get("uim_avg_day")),
                                to_bigint(metrics.get("ucl_avg_day")),

                                *[to_bigint(metrics.get(f"imp_{hour}")) for hour in HOUR_FIELDS],
                                *[to_bigint(metrics.get(f"clk_{hour}")) for hour in HOUR_FIELDS],

                                pre_parse_raw_text,
                                etl_datetime
                            ]
                            campaign_data.append(write_row)

                        time.sleep(API_CONFIG["request_interval"])

                    # ===================== 【关键】捕获所有异常并打印详细堆栈 =====================
                    except Exception as e:
                        print(f"❌ 接口请求失败 | campaign={camp_id}")
                        print(f"❌ 错误信息：{str(e)}")
                        print(f"❌ 完整堆栈：")
                        traceback.print_exc()
                        time.sleep(API_CONFIG["request_interval"])
                        continue

    return campaign_data


# ===================== ODPS 写入（带批次时间打印） =====================
def write_to_odps_partition(table_name: str, data: List[List]):
    if not data:
        print(f"⚠️ 分区{DT}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    # 修复：检查表是否存在的正确方法 - exist_table
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{DT}'"
    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size

    try:
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 已清空分区：{DT}")

        print(f"📊 分区{DT} - 总数据量{total_count}条，分{batch_num}批次写入")
        total_batch_time = 0

        for i in range(batch_num):
            batch_start_time = time.time()
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_count)
            batch_data = data[start_idx:end_idx]

            with table.open_writer(
                    partition=partition_spec,
                    create_partition=True
            ) as writer:
                writer.write(batch_data)

            batch_cost = round(time.time() - batch_start_time, 2)
            total_batch_time += batch_cost
            print(f"💾 批次{i + 1}/{batch_num} 入库完成 | 条数={len(batch_data)} | 耗时={batch_cost}s")

        print(f"✅ 分区{DT}全部写入完成，总入库耗时{round(total_batch_time, 2)}秒")

    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    try:
        task_start_time = time.time()
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"目标分区：{DT} | 表：{TARGET_TABLE}")

        # 1. 获取Token
        token = get_miaozhen_token()
        print(f"✅ Token获取成功")

        # 2. 获取活动列表（已过滤日期）
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            print("⚠️ 无符合日期条件的活动，任务结束")
            return

        print(f"\n========== 处理分区：{DT} ==========")
        daily_write_data = []

        # 3. 并行解析活动数据
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["max_workers"]) as executor:
            future_to_campaign = {
                executor.submit(parse_single_campaign, token, campaign): campaign
                for campaign in campaign_list
            }

            for future in as_completed(future_to_campaign):
                try:
                    campaign_data = future.result()
                    if campaign_data:
                        daily_write_data.extend(campaign_data)
                except Exception as e:
                    print(f"❌ 活动解析失败：{str(e)}")
                    traceback.print_exc()
                    continue

        # 4. 写入ODPS
        if daily_write_data:
            write_to_odps_partition(TARGET_TABLE, daily_write_data)
        else:
            print(f"⚠️ 无有效数据可写入ODPS")

        gc.enable()
        gc.collect()

        task_cost_time = round(time.time() - task_start_time, 2)
        print(f"\n===== 任务全部完成 =====")
        print(f"📈 总耗时：{task_cost_time} 秒")

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()