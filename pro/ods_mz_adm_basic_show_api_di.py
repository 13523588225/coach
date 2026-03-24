# -*- coding: utf-8 -*-
"""
秒针广告API采集 - 纯MaxCompute版
✅ API采集并发：10
✅ ODPS写入并发：3（可自由调整）
✅ 无本地保存
✅ raw_response 为空
"""
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from odps import ODPS
from odps import errors

# 关闭SSL警告
urllib3.disable_warnings(InsecureRequestWarning)

# ===================== 核心配置 =====================
ODPS_PROJECT = "coach_marketing_hub_dev"
DT = "20260301"

# ===================== 并发数配置（你问的就是这里） =====================
API_WORKERS = 10          # API采集并发数（同时请求10个接口）
ODPS_WRITE_WORKERS = 3    # MaxCompute写入并发数（同时3个线程写入）
BATCH_SIZE = 1000         # 每批次写入条数

# ===================== 业务配置 =====================
CONFIG = {
    "odps_table": "ods_mz_adm_basic_show_api_di",
    "report_params": {
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
        "timeout": 60
    }
}

all_data = []
total_collected = 0

# ===================== 工具方法 =====================
def get_log():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_str(val):
    if val is None or val == "" or val == "-" or str(val).lower() in ["null", "undefined"]:
        return ""
    return str(val).replace("\n", " ").replace("\r", "")

def format_date(dt):
    return datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")

def is_in_range(start, end, check):
    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        c = datetime.strptime(check, "%Y-%m-%d")
        return s <= c <= e
    except:
        return False

# ===================== ODPS 写入（支持并发） =====================
def write_batch(batch_data, partition_spec):
    o = ODPS(project=ODPS_PROJECT)
    table = o.get_table(CONFIG["odps_table"])
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(batch_data)

def write_to_odps_partition(table_name: str, data: List[List]):
    if not data:
        print(f"⚠️ 分区{DT}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    table = o.get_table(table_name)
    partition_spec = f"dt='{DT}'"
    total_count = len(data)
    batch_num = (total_count + BATCH_SIZE - 1) // BATCH_SIZE

    try:
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 已清空分区：{DT}")

        print(f"📊 总数据{total_count}条，分{batch_num}批次，ODPS写入并发={ODPS_WRITE_WORKERS}")

        batches = [data[i*BATCH_SIZE : (i+1)*BATCH_SIZE] for i in range(batch_num)]
        total_batch_time = time.time()

        with ThreadPoolExecutor(max_workers=ODPS_WRITE_WORKERS) as executor:
            for idx, batch in enumerate(batches):
                executor.submit(write_batch, batch, partition_spec)
                print(f"💾 批次 {idx+1}/{len(batches)} 提交写入")

        print(f"✅ 分区{DT}全部写入完成，总耗时{round(time.time() - total_batch_time, 2)}s")

    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")

# ===================== API 认证 =====================
def get_token():
    s = time.time()
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"[{get_log()}] 🔑 获取TOKEN成功")
    return token

# ===================== 获取活动列表 =====================
def get_campaigns(token):
    resp = requests.get(f"{CONFIG['api']['campaign_url']}?access_token={token}", timeout=60, verify=False)
    resp.raise_for_status()
    camps = []
    for item in resp.json():
        cid = item.get("campaign_id")
        sdt = item.get("start_date")
        edt = item.get("end_date")
        if cid and sdt and edt:
            camps.append({"campaign_id": str(cid), "start_date": sdt, "end_date": edt})
    print(f"[{get_log()}] 📋 有效活动 {len(camps)} 个")
    return camps

# ===================== 拉取报表 =====================
def fetch_task(task, token, dt):
    global total_collected
    camp, reg, aud, plt, pos = task
    cid = camp["campaign_id"]
    if not is_in_range(camp["start_date"], camp["end_date"], dt):
        return

    params = {
        "access_token": token, "campaign_id": cid, "date": dt, "metrics": "all",
        "by_region": reg, "by_audience": aud, "platform": plt, "by_position": pos
    }

    try:
        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        data = resp.json()
        items = data.get("items", [])
        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
            if not metric:
                continue

            row = [
                safe_str(cid), safe_str(camp["start_date"]), safe_str(camp["end_date"]), safe_str(data.get("date")),
                safe_str(pos), safe_str(reg), "all", safe_str(aud), safe_str(plt), safe_str(data.get("version")),
                safe_str(data.get("platform")), safe_str(data.get("total_spot_num")),
                safe_str(attr.get("audience")), safe_str(attr.get("target_id")), safe_str(attr.get("publisher_id")),
                safe_str(attr.get("spot_id")), safe_str(attr.get("keyword_id", "")), safe_str(attr.get("region_id")),
                safe_str(attr.get("universe")),
                safe_str(metric.get("imp_acc")), safe_str(metric.get("clk_acc")), safe_str(metric.get("uim_acc")),
                safe_str(metric.get("ucl_acc")),
                safe_str(metric.get("imp_day")), safe_str(metric.get("clk_day")), safe_str(metric.get("uim_day")),
                safe_str(metric.get("ucl_day")),
                safe_str(metric.get("imp_avg_day")), safe_str(metric.get("clk_avg_day")),
                safe_str(metric.get("uim_avg_day")), safe_str(metric.get("ucl_avg_day")),
                safe_str(metric.get("imp_h00")), safe_str(metric.get("imp_h23")), safe_str(metric.get("clk_h00")),
                safe_str(metric.get("clk_h23")), safe_str(metric.get("imp_h01", "")),
                safe_str(metric.get("imp_h02", "")), safe_str(metric.get("clk_h01", "")),
                safe_str(metric.get("clk_h02", "")),
                json.dumps(params, ensure_ascii=False),
                "",
                get_log()
            ]
            all_data.append(row)
            total_collected += 1

        print(f"[{get_log()}] 📥 {cid} 完成 | 总计：{total_collected}")
    except Exception as e:
        print(f"[{get_log()}] ❌ {cid} 失败：{str(e)[:80]}")

# ===================== 打印表结构 =====================
def print_table_schema():
    print("\n" + "="*80)
    print("📋 最终写入 MaxCompute 表结构")
    print("="*80)
    schema = [
        "campaign_id","start_date","end_date","date","by_position","by_region","all_flag","by_audience","platform",
        "version","report_platform","total_spot_num","attr_audience","attr_target_id","attr_publisher_id",
        "attr_spot_id","attr_keyword_id","attr_region_id","attr_universe","metric_imp_acc","metric_clk_acc",
        "metric_uim_acc","metric_ucl_acc","metric_imp_day","metric_clk_day","metric_uim_day","metric_ucl_day",
        "metric_imp_avg_day","metric_clk_avg_day","metric_uim_avg_day","metric_ucl_avg_day","metric_imp_h00",
        "metric_imp_h23","metric_clk_h00","metric_clk_h23","metric_imp_h01","metric_imp_h02","metric_clk_h01",
        "metric_clk_h02","request_params","raw_response","collect_time"
    ]
    for i, f in enumerate(schema, 1):
        print(f"{i:2d}. {f}")

# ===================== 主函数 =====================
def main():
    check_dt = format_date(DT)
    print(f"[{get_log()}] 🚀 开始采集 | API并发={API_WORKERS} | ODPS写入并发={ODPS_WRITE_WORKERS}")

    token = get_token()
    camps = get_campaigns(token)
    if not camps:
        return

    tasks = []
    for c in camps:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((c, r, a, p, pos))

    with ThreadPoolExecutor(API_WORKERS) as pool:
        futures = [pool.submit(fetch_task, t, token, check_dt) for t in tasks]
        wait(futures)

    write_to_odps_partition(CONFIG["odps_table"], all_data)
    print_table_schema()

    print("\n" + "="*60)
    print(f"🎉 任务完成")
    print(f"采集条数：{total_collected}")
    print(f"API并发：{API_WORKERS}")
    print(f"ODPS写入并发：{ODPS_WRITE_WORKERS}")
    print("="*60)

if __name__ == "__main__":
    main()