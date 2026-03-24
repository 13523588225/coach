# -*- coding: utf-8 -*-
"""
秒针广告API数据采集 - 生产稳定版
API并行：10
写入并行：5
批次：20000
"""
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
import requests
import urllib3
from odps import ODPS, options

urllib3.disable_warnings(InsecureRequestWarning)
ODPS_PROJECT = ODPS().project

# ===================== 核心配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",
        "batch_size": 20000,
        "dt": "20260301",
        "write_workers": 5    # 写入并行 5
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
        "campaign_url": "https://api.cn.miaozhen.com/campaign/v1/list",
        "report_url": "https://api.cn.miaozhen.com/report/v1/show",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 60,
        "api_workers": 10  # API并行 10
    }
}

# ===================== 工具 =====================
def get_log_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def safe_str(val):
    if val is None or val == "" or val == "-" or val in ("null", "undefined"):
        return None
    return str(val).replace("\n", " ").replace("\r", "")

def get_etl_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_date(dt):
    return datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")

def is_in_date_range(start, end, target):
    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        t = datetime.strptime(target, "%Y-%m-%d")
        return s <= t <= e
    except:
        return False

# ===================== 写入 =====================
writer_list = []
cache = []
cache_lock = False

def init_writers(dt):
    odps = ODPS(project=ODPS_PROJECT)
    options.tunnel.use_instance_tunnel = True
    table = odps.get_table(CONFIG["odps"]["table_name"])
    writers = []
    for i in range(CONFIG["odps"]["write_workers"]):
        w = table.open_writer(partition=f"dt='{dt}'", create_partition=True)
        writers.append(w)
    return writers

def write_worker(idx, writer, dt):
    global cache, cache_lock
    batch_size = CONFIG["odps"]["batch_size"]
    while True:
        time.sleep(0.2)
        if cache_lock:
            continue
        if len(cache) >= batch_size:
            cache_lock = True
            batch = cache[:batch_size]
            del cache[:batch_size]
            cache_lock = False

            s = time.time()
            writer.write(batch)
            cost = round(time.time() - s, 3)
            print(f"[{get_log_time()}] ✅ 写入线程{idx} | 批次{len(batch)}条 | 耗时{cost}s")

        if not cache and cache_lock is False:
            break

# ===================== API采集 =====================
token = None
camp_list = None
dt = None
check_date = None

def get_token():
    s = time.time()
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    cost = round(time.time() - s, 3)
    print(f"[{get_log_time()}] 🔑 获取TOKEN成功 | 耗时{cost}s")
    return resp.json()["access_token"]

def get_campaign_list(tk):
    s = time.time()
    resp = requests.get(f"{CONFIG['api']['campaign_url']}?access_token={tk}", timeout=60, verify=False)
    resp.raise_for_status()
    cost = round(time.time() - s, 3)
    campaigns = []
    for item in resp.json():
        cid = item.get("campaign_id")
        sdt = item.get("start_date")
        edt = item.get("end_date")
        if cid and sdt and edt:
            campaigns.append({"campaign_id": str(cid), "start_date": sdt, "end_date": edt})
    print(f"[{get_log_time()}] 📋 活动数量 {len(campaigns)} | 耗时{cost}s")
    return campaigns

def fetch_task(task):
    global cache, cache_lock
    camp, r, a, p, pos = task
    cid = camp["campaign_id"]
    sdt = camp["start_date"]
    edt = camp["end_date"]

    if not is_in_date_range(sdt, edt, check_date):
        return

    params = {
        "access_token": token,
        "campaign_id": cid,
        "date": check_date,
        "metrics": "all",
        "by_region": r,
        "by_audience": a,
        "platform": p,
        "by_position": pos
    }

    try:
        s = time.time()
        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        cost = round(time.time() - s, 3)
        data = resp.json()
        items = data.get("items", [])

        rows = []
        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
            row_raw = json.dumps(item, ensure_ascii=False)
            row = [
                safe_str(cid), safe_str(sdt), safe_str(edt), safe_str(data.get("date")),
                safe_str(pos), safe_str(r), "all", safe_str(a), safe_str(p),
                safe_str(data.get("s_version")), safe_str(data.get("platform")), safe_str(data.get("total_spot_num")),
                safe_str(attr.get("audience")), None, safe_str(attr.get("publisher_id")), safe_str(attr.get("spot_id")),
                None, safe_str(attr.get("region_id")), safe_str(attr.get("universe")),
                safe_str(metric.get("imp_acc")), safe_str(metric.get("clk_acc")), safe_str(metric.get("uim_acc")), safe_str(metric.get("ucl_acc")),
                safe_str(metric.get("imp_day")), safe_str(metric.get("clk_day")), safe_str(metric.get("uim_day")), safe_str(metric.get("ucl_day")),
                safe_str(metric.get("imp_avg_day")), safe_str(metric.get("clk_avg_day")), safe_str(metric.get("uim_avg_day")), safe_str(metric.get("ucl_avg_day")),
                safe_str(metric.get("imp_h00")), safe_str(metric.get("imp_h23")),
                safe_str(metric.get("clk_h00")), safe_str(metric.get("clk_h23")),
                json.dumps(params, ensure_ascii=False),
                row_raw,
                get_etl_time()
            ]
            rows.append(row)

        while cache_lock:
            time.sleep(0.05)
        cache_lock = True
        cache.extend(rows)
        cache_lock = False

        print(f"[{get_log_time()}] 📥 {cid} | {pos}-{r}-{a}-{p} | {len(rows)}条 | 耗时{cost}s")
    except Exception:
        return

# ===================== 主函数 =====================
def main():
    global token, camp_list, dt, check_date
    dt = CONFIG["odps"]["dt"]
    check_date = format_date(dt)
    print(f"[{get_log_time()}] 🚀 开始任务 | API并行10 | 写入并行5 | 批次20000 | dt={dt}")

    token = get_token()
    camp_list = get_campaign_list(token)

    # 构造任务
    tasks = []
    for camp in camp_list:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((camp, r, a, p, pos))

    # 启动写入线程
    writers = init_writers(dt)
    write_futures = []
    with ThreadPoolExecutor(max_workers=CONFIG["odps"]["write_workers"]) as write_pool:
        for i, w in enumerate(writers):
            f = write_pool.submit(write_worker, i, w, dt)
            write_futures.append(f)

        # 启动API并行
        with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as api_pool:
            futures = [api_pool.submit(fetch_task, t) for t in tasks]
            wait(futures)

        # 等待写入完成
        wait(write_futures)

    # 最后剩余写入
    time.sleep(1)
    if cache:
        print(f"[{get_log_time()}] ✍️  写入最后剩余 {len(cache)} 条")
        writers[0].write(cache)

    for w in writers:
        w.close()

    print(f"[{get_log_time()}] 🎉 全部完成")

if __name__ == "__main__":
    main()