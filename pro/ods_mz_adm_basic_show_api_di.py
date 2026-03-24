# -*- coding: utf-8 -*-
"""
秒针广告API数据采集脚本（防OOM终极版）
解决：内存溢出、边采边写、稳定运行
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue

import requests
import urllib3
from odps import ODPS, options

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ODPS_PROJECT = ODPS().project

# ===================== 【防OOM】最终安全配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",
        "batch_size": 2000,    # 关键：降低批次，防OOM
        "write_workers": 4,
        "dt": "20260301"
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
        "interval": 0.01,
        "api_workers": 5       # 关键：降低并发，防内存瞬间暴涨
    }
}

# ===================== 【防OOM】队列大小缩小 =====================
write_queue = Queue(maxsize=2000)
write_finished = False
total_written = 0
total_collected = 0

# ===================== 工具方法 =====================
def safe_str(val):
    if val is None or val == "" or val == "-" or val in ("null", "undefined"):
        return None
    return str(val)

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
            print(f"✅ 写入分区 {partition_dt} | 批次 {len(batch)} 条 | 耗时 {cost_time}s | 累计 {total_written}")

# ===================== API 采集 =====================
def get_miaozhen_token():
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_valid_campaign_list(token):
    resp = requests.get(f"{CONFIG['api']['campaign_url']}?access_token={token}", timeout=60, verify=False)
    resp.raise_for_status()
    campaign_list = []
    for camp in resp.json():
        camp_id = camp.get("campaign_id")
        start_date = camp.get("start_date")
        end_date = camp.get("end_date")
        if camp_id and start_date and end_date:
            campaign_list.append({"campaign_id": str(camp_id), "start_date": start_date, "end_date": end_date})
    print(f"✅ 有效活动：{len(campaign_list)} 个")
    return campaign_list

def collect_report_data(token, campaign, check_date, by_region, by_audience, platform, by_position):
    global total_collected
    camp_id = campaign["campaign_id"]
    camp_start = campaign["start_date"]
    camp_end = campaign["end_date"]

    if not is_campaign_include_date(camp_start, camp_end, check_date):
        print(f"ℹ️ 活动 {camp_id} 日期不在范围内，跳过")
        return

    # ===================== 【防OOM】核心：队列满就等待 =====================
    while write_queue.qsize() > 1800:
        time.sleep(0.05)

    try:
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

        api_start = time.time()
        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        api_cost = round(time.time() - api_start, 3)

        data = resp.json()
        rows = []
        for item in data.get("items", []):
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
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
                safe_str(resp.text),
                get_etl_datetime()
            ]
            rows.append(row)

        total_collected += len(rows)
        for r in rows:
            write_queue.put(r)

        print(f"📥 采集 {camp_id} | 条数 {len(rows)} | API耗时 {api_cost}s")

    except Exception as e:
        print(f"❌ 失败 {camp_id}：{str(e)}")

# ===================== 主流程 =====================
def main():
    global write_finished
    print("=" * 80)
    print("🚀 防OOM稳定版 | 固定分区 dt=20260301")
    print("=" * 80)

    DEFAULT_DT = CONFIG["odps"]["dt"]
    check_date = format_date(DEFAULT_DT)
    partition_dt = DEFAULT_DT

    token = get_miaozhen_token()
    campaign_list = get_valid_campaign_list(token)
    odps_writer = init_odps_writer(partition_dt)

    tasks = []
    for camp in campaign_list:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((token, camp, check_date, r, a, p, pos))

    print(f"🧩 总任务数：{len(tasks)}")

    write_thread = threading.Thread(target=write_worker, args=(odps_writer, partition_dt), daemon=True)
    write_thread.start()

    with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as executor:
        futures = [executor.submit(collect_report_data, *t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    write_finished = True
    write_thread.join()
    odps_writer.close()

    print("\n✅ 完成")
    print(f"分区：{partition_dt}")
    print(f"采集：{total_collected} 条")
    print(f"写入：{total_written} 条")

if __name__ == "__main__":
    main()