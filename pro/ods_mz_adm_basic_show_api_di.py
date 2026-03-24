# -*- coding: utf-8 -*-
"""
秒针广告API采集 - 本地CSV + MaxCompute规范批次写入
✅ 严格按你提供的 ODPS 写入格式
✅ 分批次写入 + 打印每批次耗时
✅ 先删分区再写入
✅ raw_response 标记为空（不存原始响应）
"""
import json
import time
import csv
import os
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
# MaxCompute 项目名（请确认你的项目名）
ODPS_PROJECT = "your_project_name"
DT = "20260301"
PARALLEL_CONFIG = {
    "batch_size": 1000  # 每批次写入1000条
}

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
        "timeout": 60,
        "api_workers": 10
    },
    "local_save": {
        "save_path": "./data",
        "file_name": f"mz_adm_basic_show_{DT}.csv"
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

# ===================== 本地保存 =====================
def save_to_local(dt, data_list):
    if not data_list:
        print("❌ 无有效数据可保存到本地")
        return

    save_dir = CONFIG["local_save"]["save_path"]
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    file_path = os.path.join(save_dir, CONFIG["local_save"]["file_name"])

    headers = [
        "campaign_id", "start_date", "end_date", "date", "by_position", "by_region",
        "all_flag", "by_audience", "platform", "version", "report_platform", "total_spot_num",
        "attr_audience", "attr_target_id", "attr_publisher_id", "attr_spot_id", "attr_keyword_id",
        "attr_region_id", "attr_universe", "metric_imp_acc", "metric_clk_acc", "metric_uim_acc",
        "metric_ucl_acc", "metric_imp_day", "metric_clk_day", "metric_uim_day", "metric_ucl_day",
        "metric_imp_avg_day", "metric_clk_avg_day", "metric_uim_avg_day", "metric_ucl_avg_day",
        "metric_imp_h00", "metric_imp_h23", "metric_clk_h00", "metric_clk_h23", "metric_imp_h01",
        "metric_imp_h02", "metric_clk_h01", "metric_clk_h02", "request_params", "raw_response",
        "collect_time"
    ]

    try:
        with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data_list)
        print(f"[{get_log()}] 💾 本地保存成功：{file_path}")
    except Exception as e:
        print(f"[{get_log()}] ❌ 本地保存失败：{str(e)}")

# ===================== ODPS 写入（完全按你给的格式） =====================
def write_to_odps_partition(table_name: str, data: List[List]):
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

# ===================== API 认证 =====================
def get_token():
    s = time.time()
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"[{get_log()}] 🔑 获取TOKEN成功，耗时 {round(time.time() - s, 2)}s")
    return token

# ===================== 获取活动列表 =====================
def get_campaigns(token):
    s = time.time()
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
        "access_token": token,
        "campaign_id": cid,
        "date": dt,
        "metrics": "all",
        "by_region": reg,
        "by_audience": aud,
        "platform": plt,
        "by_position": pos
    }

    try:
        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])

        rows = []
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
                "",  # <-- raw_response 已置空
                get_log()
            ]
            rows.append(row)

        all_data.extend(rows)
        total_collected += len(rows)
        print(f"[{get_log()}] 📥 {cid} | {len(rows)} 条 | 总计：{total_collected}")

    except Exception as e:
        print(f"[{get_log()}] ❌ {cid} 失败：{str(e)[:100]}")
        return

# ===================== 主函数 =====================
def main():
    check_dt = format_date(DT)
    print(f"[{get_log()}] 🚀 开始采集（本地+MaxCompute双输出）")

    token = get_token()
    camps = get_campaigns(token)
    if not camps:
        print("❌ 无有效活动")
        return

    tasks = []
    for c in camps:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((c, r, a, p, pos))

    with ThreadPoolExecutor(CONFIG["api"]["api_workers"]) as pool:
        futures = [pool.submit(fetch_task, t, token, check_dt) for t in tasks]
        wait(futures)

    # 输出
    save_to_local(DT, all_data)
    write_to_odps_partition(CONFIG["odps_table"], all_data)

    print("\n" + "=" * 60)
    print(f"[{get_log()}] 🎉 任务全部完成")
    print(f"采集条数：{total_collected}")
    print(f"本地文件：./data/{CONFIG['local_save']['file_name']}")
    print(f"MaxCompute：{CONFIG['odps_table']}  dt={DT}")
    print("=" * 60)

if __name__ == "__main__":
    main()