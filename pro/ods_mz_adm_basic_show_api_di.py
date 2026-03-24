# -*- coding: utf-8 -*-
"""
秒针广告API数据采集 - 生产最终版
功能：
1. 写入 MaxCompute
2. 分区写入并行度：3
3. 仅存储本行解析对应原始数据片段
4. 无多余日志打印
5. 活动时间严格校验
6. 防OOM + 高并发
"""
import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue

import requests
import urllib3
from odps import ODPS, options

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ODPS_PROJECT = ODPS().project

# ===================== 配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",
        "batch_size": 2000,
        "dt": "20260301",
        "write_workers": 3  # 分区写入并行度 = 3
    },
    "report_params": {
        "metrics": "all",
        "by_region_list": ["level0", "level1", "level2"],
        "by_audience_list": ["overall", "stable", "target"],
        "platform_list": ["pc", "pm", "mb"],
        "by_position_list": ["campaign", "publisher", "spot", "keyword"]
    },
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "report_url": "https://api.cn.miaozhen.com/admonitor/v1/reports/basic/show",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 60,
        "api_workers": 5
    }
}

# ===================== 全局变量 =====================
write_queue = Queue(maxsize=2000)
write_finished = False
total_written = 0
total_collected = 0
skip_campaign_count = 0

# ===================== 工具方法 =====================
def safe_str(val):
    if val is None or val == "" or val == "-" or val in ("null", "undefined"):
        return None
    return str(val).replace("\n", " ").replace("\r", "")

def get_etl_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_date(date_ymd):
    return datetime.strptime(date_ymd, "%Y%m%d").strftime("%Y-%m-%d")

def is_campaign_include_date(camp_start_date, camp_end_date, check_date):
    try:
        camp_start = datetime.strptime(camp_start_date, "%Y-%m-%d")
        camp_end = datetime.strptime(camp_end_date, "%Y-%m-%d")
        check = datetime.strptime(check_date, "%Y-%m-%d")
        return camp_start <= check <= camp_end
    except Exception:
        return False

# ===================== MaxCompute 写入 =====================
def init_odps_writer(partition_dt):
    odps = ODPS(project=ODPS_PROJECT)
    options.tunnel.use_instance_tunnel = True
    table = odps.get_table(CONFIG["odps"]["table_name"])
    partition_spec = f"dt='{partition_dt}'"
    if table.exist_partition(partition_spec):
        odps.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION ({partition_spec})")
    return table.open_writer(partition=partition_spec, create_partition=True)

def write_worker(odps_writer, partition_dt):
    global total_written
    batch_size = CONFIG["odps"]["batch_size"]
    while not (write_finished and write_queue.empty()):
        batch = []
        while len(batch) < batch_size and not write_queue.empty():
            batch.append(write_queue.get())
        if batch:
            start_time = time.time()
            odps_writer.write(batch)
            cost_time = round(time.time() - start_time, 3)
            total_written += len(batch)

def write_worker_pool(partition_dt):
    writers = [init_odps_writer(partition_dt) for _ in range(CONFIG["odps"]["write_workers"])]
    with ThreadPoolExecutor(max_workers=CONFIG["odps"]["write_workers"]) as executor:
        for w in writers:
            executor.submit(write_worker, w, partition_dt)

# ===================== API 采集 =====================
def get_miaozhen_token():
    try:
        resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as e:
        raise

def get_valid_campaign_list(token):
    try:
        resp = requests.get(f"{CONFIG['api']['campaign_url']}?access_token={token}", timeout=60, verify=False)
        resp.raise_for_status()
        campaign_list = []
        for camp in resp.json():
            camp_id = camp.get("campaign_id")
            start_date = camp.get("start_date")
            end_date = camp.get("end_date")
            if camp_id and start_date and end_date:
                campaign_list.append({"campaign_id": str(camp_id), "start_date": start_date, "end_date": end_date})
        return campaign_list
    except Exception as e:
        raise

def collect_report_data(token, campaign, check_date, by_region, by_audience, platform, by_position):
    global total_collected, skip_campaign_count
    camp_id = campaign["campaign_id"]
    camp_start = campaign["start_date"]
    camp_end = campaign["end_date"]

    if not is_campaign_include_date(camp_start, camp_end, check_date):
        skip_campaign_count += 1
        return

    while write_queue.qsize() > 1800:
        time.sleep(0.05)

    params = {
        "access_token": token,
        "campaign_id": camp_id,
        "date": check_date,
        "metrics": "all",
        "by_region": by_region,
        "by_audience": by_audience,
        "platform": platform,
        "by_position": by_position
    }

    try:
        resp = requests.get(CONFIG['api']['report_url'], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
            row_raw_data = json.dumps(item, ensure_ascii=False)

            row = [
                safe_str(camp_id), safe_str(camp_start), safe_str(camp_end), safe_str(data.get("date")),
                safe_str(by_position), safe_str(by_region), safe_str("all"), safe_str(by_audience), safe_str(platform),
                safe_str(data.get("s_version")), safe_str(data.get("platform")), safe_str(data.get("total_spot_num")),
                safe_str(attr.get("audience")), None, safe_str(attr.get("publisher_id")), safe_str(attr.get("spot_id")),
                None, safe_str(attr.get("region_id")), safe_str(attr.get("universe")),
                safe_str(metric.get("imp_acc")), safe_str(metric.get("clk_acc")), safe_str(metric.get("uim_acc")), safe_str(metric.get("ucl_acc")),
                safe_str(metric.get("imp_day")), safe_str(metric.get("clk_day")), safe_str(metric.get("uim_day")), safe_str(metric.get("ucl_day")),
                safe_str(metric.get("imp_avg_day")), safe_str(metric.get("clk_avg_day")), safe_str(metric.get("uim_avg_day")), safe_str(metric.get("ucl_avg_day")),
                safe_str(metric.get("imp_h00")), safe_str(metric.get("imp_h23")),
                safe_str(metric.get("clk_h00")), safe_str(metric.get("clk_h23")),
                safe_str(json.dumps(params, ensure_ascii=False)),
                row_raw_data,
                get_etl_datetime()
            ]
            write_queue.put(row)
            total_collected += 1

    except Exception:
        pass

# ===================== 主流程 =====================
def main():
    global write_finished
    dt = CONFIG["odps"]["dt"]
    check_date = format_date(dt)
    token = get_miaozhen_token()
    campaign_list = get_valid_campaign_list(token)

    tasks = []
    for camp in campaign_list:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((token, camp, check_date, r, a, p, pos))

    # 启动 3 线程并行写入分区
    write_thread = threading.Thread(target=write_worker_pool, args=(dt,), daemon=True)
    write_thread.start()

    # API 采集并发 5
    with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as executor:
        futures = [executor.submit(collect_report_data, *t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    write_finished = True
    write_thread.join()

if __name__ == "__main__":
    main()