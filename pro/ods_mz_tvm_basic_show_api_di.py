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

# ===================== 全局配置 =====================
urllib3.disable_warnings(InsecureRequestWarning)

# 1. 秒针接口配置（移除campaign_list_url，保留其他）
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


def get_log() -> str:
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
            print(f"[{get_log()}] 🔐 成功获取TVM账号：{username}")
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


# ===================== 获取活动列表（修改核心逻辑：从ODPS查询） =====================
def get_campaign_list() -> List[Dict]:
    try:
        o = ODPS(project=ODPS_PROJECT)
        # 构造查询SQL：按业务日期筛选活动
        sql = """
              select campaign_id, start_time, end_time
              from ods_mz_tvm_campaigns_list_api_df 
              where args['dt'] between replace(start_time, '-', '') and replace(end_time, '-', '')
              """
        print(f"[{get_log()}] 📝 执行SQL：{sql}")

        valid_campaigns = []
        with o.execute_sql(sql).open_reader() as reader:
            for record in reader:
                # 直接取值并转换字符串
                campaign_id = to_string(record.campaign_id)
                # 无活动ID则跳过
                if not campaign_id:
                    continue
                # 直接组装数据并添加
                valid_campaigns.append({
                    "campaign_id": campaign_id,
                    "camp_start_date": to_string(record.start_time),
                    "camp_end_date": to_string(record.end_time)
                })

        print(f"✅ 有效活动数：{len(valid_campaigns)}")
        return valid_campaigns
    except errors.ODPSError as e:
        raise Exception(f"查询活动列表失败：{str(e)}")
    except Exception as e:
        raise Exception(f"处理活动列表失败：{str(e)}")


# ===================== 解析单活动数据（严格对齐表结构） =====================
def parse_single_campaign(token: str, campaign: Dict) -> List[List]:
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]
    report_date_10bit = date_convert(DT, "10位")
    if not report_date_10bit:
        return campaign_data

    # 无platform循环
    for by_region in REPORT_PARAMS["by_region"]:
        for by_audience in REPORT_PARAMS["by_audience"]:
            for by_position in REPORT_PARAMS["by_position"]:
                try:
                    # 请求参数
                    request_params = {
                        "access_token": token,
                        "campaign_id": camp_id,
                        "date": report_date_10bit,
                        "metrics": REPORT_PARAMS["metrics"],
                        "by_region": by_region,
                        "by_audience": by_audience,
                        "by_position": by_position
                    }
                    full_request_url = f"{API_CONFIG['report_basic_url']}?{urlencode(request_params)}"

                    # 接口请求
                    resp = SESSION.get(
                        API_CONFIG['report_basic_url'],
                        params=request_params,
                        timeout=API_CONFIG["timeout"],
                        verify=False
                    )
                    resp.raise_for_status()
                    raw_data = resp.json()

                    if raw_data.get("error_code") != 0:
                        print(f"⚠️ 接口错误：{full_request_url}")
                        time.sleep(API_CONFIG["request_interval"])
                        continue

                    result = raw_data.get("result", {})
                    items = result.get("items", [])
                    if not items:
                        time.sleep(API_CONFIG["request_interval"])
                        continue

                    etl_datetime_str = get_etl_time()
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        attributes = item.get("attributes", {})
                        metrics = item.get("metrics", {}) or {}

                        # 原始数据
                        pre_parse_raw_text = to_string(
                            json.dumps({"attributes": attributes, "metrics": metrics}, ensure_ascii=False)
                        )

                        # ===================== 严格对齐最新表结构字段顺序 =====================
                        write_row = [
                            # 1. 请求参数
                            to_string(request_params["campaign_id"]),
                            to_string(request_params["date"]),
                            to_string(request_params["metrics"]),
                            to_string(request_params["by_position"]),
                            to_string(request_params["by_region"]),
                            to_string(request_params["by_audience"]),

                            # 2. 业务基础字段
                            to_string(result.get("campaignId")),
                            to_string(camp_start),
                            to_string(camp_end),
                            to_string(result.get("date")),  # result_date 对齐表结构
                            to_bigint(result.get("version")),
                            to_string(attributes.get("publisher_id")),
                            to_string(attributes.get("spot_id")),
                            to_string(attributes.get("spot_id_str")),
                            to_string(attributes.get("audience")),
                            to_string(attributes.get("universe")),
                            to_string(attributes.get("region_id")),
                            to_string(attributes.get("target_def")),
                            to_string(attributes.get("target_name")),
                            to_string(attributes.get("target_id")),

                            # 3. 核心指标
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

                            # 4. 24小时曝光
                            *[to_bigint(metrics.get(f"imp_{hour}")) for hour in HOUR_FIELDS],
                            # 5. 24小时点击
                            *[to_bigint(metrics.get(f"clk_{hour}")) for hour in HOUR_FIELDS],

                            # 6. 新增字段
                            to_string(full_request_url),
                            pre_parse_raw_text,
                            to_string(etl_datetime_str)
                        ]
                        campaign_data.append(write_row)

                    time.sleep(API_CONFIG["request_interval"])
                except Exception as e:
                    print(f"❌ 请求失败：{full_request_url}，错误：{str(e)}")
                    traceback.print_exc()
                    time.sleep(API_CONFIG["request_interval"])
                    continue
    return campaign_data


# ===================== 写入ODPS =====================
def write_to_odps_partition(table_name: str, data: List[List]):
    if not data:
        print(f"⚠️ 无数据写入")
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
        # 清空旧分区
        if table.exist_partition(partition_spec):
            o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{DT}")

        print(f"📊 总条数：{total_count}，分{batch_num}批次写入")
        total_time = 0
        for i in range(batch_num):
            start = time.time()
            batch_data = data[i * batch_size: (i + 1) * batch_size]
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)
            cost = round(time.time() - start, 2)
            total_time += cost
            print(f"💾 批次{i + 1}完成，条数：{len(batch_data)}，耗时：{cost}s")

        print(f"✅ 写入完成，总耗时：{total_time}s")
    except errors.ODPSError as e:
        raise Exception(f"写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    try:
        start_time = time.time()
        print(f"===== 任务开始 | 表：{TARGET_TABLE} | 分区：{DT} =====")

        # 1. 获取Token
        token = get_miaozhen_token()
        # 2. 获取活动列表（修改：不再传token）
        campaign_list = get_campaign_list()
        if not campaign_list:
            print("⚠️ 无有效活动")
            return

        # 3. 并行处理
        all_data = []
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["max_workers"]) as executor:
            futures = [executor.submit(parse_single_campaign, token, c) for c in campaign_list]
            for future in as_completed(futures):
                try:
                    all_data.extend(future.result())
                except Exception as e:
                    print(f"❌ 解析失败：{str(e)}")

        # 4. 写入数据
        write_to_odps_partition(TARGET_TABLE, all_data)

        gc.enable()
        gc.collect()
        print(f"===== 任务完成 | 总耗时：{round(time.time() - start_time, 2)}s =====")

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()