# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import traceback
from datetime import datetime
from typing import Dict, List
from urllib.parse import urlencode
from odps import ODPS, errors
from concurrent.futures import ThreadPoolExecutor, as_completed
# 新增：线程安全统计依赖
from multiprocessing import Manager

# ===================== 全局配置 =====================
urllib3.disable_warnings(InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {"username": "", "password": ""},
    "timeout": 30,
    "request_interval": 0.003
}

# 2. ODPS配置
ODPS_PROJECT = ODPS().project
TARGET_TABLE = "ods_mz_tvm_basic_show_api_di"

# 3. 业务日期参数
DT = args['dt']

# 4. 接口维度参数
REPORT_PARAMS = {
    "metrics": "all",
    "by_region": ["level0", "level1", "level2"],
    "by_audience": ["overall", "people", "target"],
    "by_position": ["campaign", "publisher", "spot"]
}

# 5. 并行/批次配置
PARALLEL_CONFIG = {
    "max_workers": 6,
    "batch_size": 50000
}

# 6. 小时字段
HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]

# ===================== 全局优化 =====================
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


# ===================== 从MaxCompute查询API账号密码 =====================
def get_tvm_api_credentials():
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'TVM' limit 1 \
          """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_etl_time()}] 🔐 成功获取TVM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"查询账号失败：{str(e)}")


# ===================== 获取Token =====================
def get_miaozhen_token() -> str:
    try:
        username, password = get_tvm_api_credentials()
        auth_data = {"username": username, "password": password}
        resp = SESSION.post(
            API_CONFIG["token_url"],
            data=auth_data,
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


# ===================== 获取活动列表（极简版） =====================
def get_campaign_list() -> List[Dict]:
    try:
        o = ODPS(project=ODPS_PROJECT)
        sql = f"""
              select campaign_id, start_time, end_time
              from ods_mz_tvm_campaigns_list_api_df 
              where '{DT}' between replace(start_time, '-', '') and replace(end_time, '-', '')
              """
        print(f"[{get_etl_time()}] 📝 执行活动SQL：{sql}")

        valid_campaigns = []
        with o.execute_sql(sql).open_reader() as reader:
            for record in reader:
                campaign_id = to_string(record.campaign_id)
                if not campaign_id:
                    continue
                valid_campaigns.append({
                    "campaign_id": campaign_id,
                    "camp_start_date": to_string(record.start_time),
                    "camp_end_date": to_string(record.end_time)
                })

        print(f"[{get_etl_time()}] ✅ 有效活动数：{len(valid_campaigns)}")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"查询活动列表失败：{str(e)}")


# ===================== 解析单活动数据（新增统计参数） =====================
def parse_single_campaign(token: str, campaign: Dict, stats: Dict) -> List[List]:
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]
    report_date_10bit = date_convert(DT, "10位")
    if not report_date_10bit:
        return campaign_data

    # 三层维度循环 = 每个循环为一个请求批次
    for by_region in REPORT_PARAMS["by_region"]:
        for by_audience in REPORT_PARAMS["by_audience"]:
            for by_position in REPORT_PARAMS["by_position"]:
                try:
                    # 1. 构建请求参数
                    request_params = {
                        "access_token": token,
                        "campaign_id": camp_id,
                        "date": report_date_10bit,
                        "metrics": REPORT_PARAMS["metrics"],
                        "by_region": by_region,
                        "by_audience": by_audience,
                        "by_position": by_position
                    }
                    full_url = f"{API_CONFIG['report_basic_url']}?{urlencode(request_params)}"

                    # 2. 记录批次开始时间
                    req_start_time = time.time()
                    req_start_str = get_etl_time()
                    print(f"[{req_start_str}] 🚀 请求批次开始 | URL：{full_url}")

                    # 3. 发送接口请求
                    resp = SESSION.get(
                        API_CONFIG['report_basic_url'],
                        params=request_params,
                        timeout=API_CONFIG["timeout"],
                        verify=False
                    )
                    resp.raise_for_status()
                    raw_data = resp.json()

                    # 4. 计算单次请求耗时
                    cost_time = round(time.time() - req_start_time, 3)
                    error_code = raw_data.get("error_code", -1)

                    # ===================== 统计累加（核心） =====================
                    stats["total_requests"] += 1
                    stats["total_cost"] += cost_time

                    # 5. 仅处理 error_code = 0
                    if error_code != 0:
                        stats["fail_requests"] += 1
                        print(
                            f"[{get_etl_time()}] ⚠️ 批次请求失败 | 耗时：{cost_time}s | 错误码：{error_code} | URL：{full_url}")
                        time.sleep(API_CONFIG["request_interval"])
                        continue

                    # 6. 成功解析数据
                    stats["success_requests"] += 1
                    print(f"[{get_etl_time()}] ✅ 批次请求成功 | 耗时：{cost_time}s | URL：{full_url}")
                    items = raw_data.get("result", {}).get("items", [])
                    if not items:
                        time.sleep(API_CONFIG["request_interval"])
                        continue

                    etl_time = get_etl_time()
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        attributes = item.get("attributes", {})
                        metrics = item.get("metrics", {}) or {}
                        raw_text = to_string(
                            json.dumps({"attributes": attributes, "metrics": metrics}, ensure_ascii=False))

                        # 对齐表结构写入数据
                        write_row = [
                            to_string(request_params["campaign_id"]),
                            to_string(request_params["date"]),
                            to_string(request_params["metrics"]),
                            to_string(request_params["by_position"]),
                            to_string(request_params["by_region"]),
                            to_string(request_params["by_audience"]),
                            to_string(raw_data.get("result", {}).get("campaignId")),
                            to_string(camp_start),
                            to_string(camp_end),
                            to_string(raw_data.get("result", {}).get("date")),
                            to_bigint(raw_data.get("result", {}).get("version")),
                            to_string(attributes.get("publisher_id")),
                            to_string(attributes.get("spot_id")),
                            to_string(attributes.get("spot_id_str")),
                            to_string(attributes.get("audience")),
                            to_string(attributes.get("universe")),
                            to_string(attributes.get("region_id")),
                            to_string(attributes.get("target_def")),
                            to_string(attributes.get("target_name")),
                            to_string(attributes.get("target_id")),
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
                            *[to_bigint(metrics.get(f"imp_{h}")) for h in HOUR_FIELDS],
                            *[to_bigint(metrics.get(f"clk_{h}")) for h in HOUR_FIELDS],
                            to_string(full_url),
                            raw_text,
                            to_string(etl_time)
                        ]
                        campaign_data.append(write_row)

                    time.sleep(API_CONFIG["request_interval"])

                except Exception as e:
                    stats["fail_requests"] += 1
                    print(f"[{get_etl_time()}] ❌ 批次异常 | URL：{full_url} | 错误：{str(e)}")
                    time.sleep(API_CONFIG["request_interval"])
                    continue
    return campaign_data


# ===================== 写入ODPS =====================
def write_to_odps_partition(table_name: str, data: List[List]):
    if not data:
        print(f"[{get_etl_time()}] ⚠️ 无数据写入")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{DT}'"
    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size

    try:
        if table.exist_partition(partition_spec):
            o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"[{get_etl_time()}] ✅ 清空旧分区：{DT}")

        print(f"[{get_etl_time()}] 📊 总数据量：{total_count} 条，分 {batch_num} 批次写入")
        total_cost = 0
        for i in range(batch_num):
            start = time.time()
            batch_data = data[i * batch_size: (i + 1) * batch_size]
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)
            cost = round(time.time() - start, 2)
            total_cost += cost
            print(f"[{get_etl_time()}] 💾 写入批次 {i + 1} | 条数：{len(batch_data)} | 耗时：{cost}s")

        print(f"[{get_etl_time()}] ✅ 全部写入完成 | 写入总耗时：{total_cost}s")
    except errors.ODPSError as e:
        raise Exception(f"写入ODPS失败：{str(e)}")


# ===================== 主流程（新增统计初始化） =====================
def main():
    try:
        total_start = time.time()
        print(f"===== [{get_etl_time()}] 任务开始 | 表：{TARGET_TABLE} | 分区：{DT} =====")

        # 初始化线程安全的请求统计
        manager = Manager()
        request_stats = manager.dict({
            "total_requests": 0,  # 总请求数
            "success_requests": 0,  # 成功请求数
            "fail_requests": 0,  # 失败请求数
            "total_cost": 0.0  # 接口请求总耗时（核心）
        })

        # 1. 获取Token
        token = get_miaozhen_token()
        # 2. 获取活动列表
        campaign_list = get_campaign_list()
        if not campaign_list:
            print(f"[{get_etl_time()}] ⚠️ 无有效活动，任务结束")
            return

        # 3. 并行处理（传入统计参数）
        all_data = []
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["max_workers"]) as executor:
            futures = [executor.submit(parse_single_campaign, token, c, request_stats) for c in campaign_list]
            for future in as_completed(futures):
                try:
                    all_data.extend(future.result())
                except Exception as e:
                    print(f"[{get_etl_time()}] ❌ 活动解析失败：{str(e)}")

        # 4. 写入数据
        write_to_odps_partition(TARGET_TABLE, all_data)

        gc.enable()
        gc.collect()
        total_cost = round(time.time() - total_start, 2)

        # ===================== 打印最终统计（核心输出） =====================
        print(f"\n===== [{get_etl_time()}] 📊 接口请求统计汇总 =====")
        print(f"总请求次数：{request_stats['total_requests']} 次")
        print(f"请求成功次数：{request_stats['success_requests']} 次")
        print(f"请求失败次数：{request_stats['fail_requests']} 次")
        print(f"全部URL请求总耗时：{round(request_stats['total_cost'], 3)} 秒")
        print(f"===== 任务全部完成 | 总运行耗时：{total_cost}s =====")

    except Exception as e:
        print(f"[{get_etl_time()}] ❌ 任务执行失败：{str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()