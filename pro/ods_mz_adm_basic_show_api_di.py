# -*- coding: utf-8 -*-
"""
秒针广告API数据采集脚本
功能：高并发采集报表数据，边采集边写入MaxCompute，避免内存溢出
默认分区：dt=20260301
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from queue import Queue

import requests
import urllib3
from odps import ODPS, options

# 关闭SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 自动获取ODPS项目名称
ODPS_PROJECT = ODPS().project

# ===================== 全局配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",
        "partition_col": "dt",
        "batch_size": 20000,
        "write_workers": 10
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
        "api_workers": 20
    }
}

# ===================== 全局变量（边采边写） =====================
write_queue = Queue(maxsize=100000)
queue_lock = threading.Lock()
write_finished = False
total_written = 0
total_collected = 0

# ===================== 工具方法 =====================

def safe_str(val):
    """
    安全转换为字符串，空值/异常值返回None
    """
    if val is None or val == "" or val == "-" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_datetime():
    """
    获取当前采集时间，格式：yyyy-MM-dd HH:mm:ss
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_date(date_ymd):
    """
    将 yyyyMMdd 转为 yyyy-MM-dd
    """
    return datetime.strptime(date_ymd, "%Y%m%d").strftime("%Y-%m-%d")


def is_date_overlap(config_date, camp_start, camp_end):
    """
    判断活动日期是否包含当前采集日期
    """
    try:
        config_dt = datetime.strptime(config_date, "%Y-%m-%d")
        camp_s_dt = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_e_dt = datetime.strptime(camp_end, "%Y-%m-%d")
        return camp_s_dt <= config_dt <= camp_e_dt
    except:
        return False

# ===================== MaxCompute 写入模块 =====================

def init_odps_writer(partition_dt):
    """
    初始化ODPS写入器，清空旧分区
    """
    odps = ODPS(project=ODPS_PROJECT)
    options.tunnel.use_instance_tunnel = True

    table = odps.get_table(CONFIG["odps"]["table_name"])
    partition_spec = f"{CONFIG['odps']['partition_col']}='{partition_dt}'"

    if table.exist_partition(partition_spec):
        odps.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION ({partition_spec})")

    return table.open_writer(partition=partition_spec, create_partition=True)


def write_worker(odps_writer, partition_dt):
    """
    写入线程：从队列消费数据并批量写入MaxCompute
    边采边写，避免内存溢出
    """
    global total_written
    batch_size = CONFIG["odps"]["batch_size"]

    while not (write_finished and write_queue.empty()):
        batch = []
        while len(batch) < batch_size and not write_queue.empty():
            batch.append(write_queue.get())

        if batch:
            start = time.time()
            odps_writer.write(batch)
            cost = round(time.time() - start, 3)
            total_written += len(batch)
            print(f"✅ 写入分区 {partition_dt} | 批次 {len(batch)} 条 | 耗时 {cost}s | 累计 {total_written} 条")

# ===================== API 采集模块 =====================

def get_miaozhen_token():
    """
    获取秒针API访问令牌
    """
    resp = requests.post(
        CONFIG["api"]["token_url"],
        data=CONFIG["api"]["auth"],
        timeout=60,
        verify=False
    )
    return resp.json()["access_token"]


def get_campaign_list(token):
    """
    获取有效活动列表
    """
    resp = requests.get(
        f"{CONFIG['api']['campaign_url']}?access_token={token}",
        timeout=60,
        verify=False
    )
    camps = []
    for c in resp.json():
        cid = c.get("campaign_id")
        s, e = c.get("start_date"), c.get("end_date")
        if cid and s and e:
            camps.append({"campaign_id": str(cid), "s": s, "e": e})
    return camps


def collect_task(token, camp, day, by_region, by_audience, platform, by_position):
    """
    单任务采集：请求报表接口 → 解析数据 → 推入写入队列
    打印API请求耗时
    """
    global total_collected
    try:
        params = {
            "access_token": token,
            "campaign_id": camp["campaign_id"],
            "date": day,
            "metrics": "all",
            "by_region": by_region,
            "by_audience": by_audience,
            "platform": platform,
            "by_position": by_position
        }

        api_start = time.time()
        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        api_cost = round(time.time() - api_start, 3)

        data = resp.json()
        rows = []

        for item in data.get("items", []):
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})

            row = [
                safe_str(camp["campaign_id"]),
                safe_str(camp["s"]),
                safe_str(camp["e"]),
                safe_str(data.get("date")),

                safe_str(by_position),
                safe_str(by_region),
                safe_str("all"),
                safe_str(by_audience),
                safe_str(platform),

                safe_str(data.get("s_version")),
                safe_str(data.get("platform")),
                safe_str(data.get("total_spot_num")),
                safe_str(attr.get("audience")),
                None,
                safe_str(attr.get("publisher_id")),
                safe_str(attr.get("spot_id")),
                None,
                safe_str(attr.get("region_id")),
                safe_str(attr.get("universe")),

                safe_str(metric.get("imp_acc")),
                safe_str(metric.get("clk_acc")),
                safe_str(metric.get("uim_acc")),
                safe_str(metric.get("ucl_acc")),
                safe_str(metric.get("imp_day")),
                safe_str(metric.get("clk_day")),
                safe_str(metric.get("uim_day")),
                safe_str(metric.get("ucl_day")),
                safe_str(metric.get("imp_avg_day")),
                safe_str(metric.get("clk_avg_day")),
                safe_str(metric.get("uim_avg_day")),
                safe_str(metric.get("ucl_avg_day")),
                safe_str(metric.get("imp_h00")),
                safe_str(metric.get("imp_h23")),
                safe_str(metric.get("clk_h00")),
                safe_str(metric.get("clk_h23")),

                safe_str(json.dumps(params, ensure_ascii=False)),
                safe_str(resp.text),
                get_etl_datetime()
            ]
            rows.append(row)

        total_collected += len(rows)
        for r in rows:
            write_queue.put(r)

        print(f"📥 采集 {camp['campaign_id']} | {by_region} | {by_audience} | {platform} | {by_position} | 条数 {len(rows)} | 请求耗时 {api_cost}s")

    except Exception as e:
        print(f"❌ 采集失败：{str(e)}")

# ===================== 主流程 =====================

def main():
    global write_finished

    print("=" * 90)
    print("🚀 秒针API高并发采集 + 边采边写MaxCompute（无内存溢出）")
    print("默认日期：dt=20260301")
    print("=" * 90)

    # 默认固定日期
    DEFAULT_DT = "20260301"
    config_day = format_date(DEFAULT_DT)
    partition_dt = DEFAULT_DT

    # 初始化
    token = get_miaozhen_token()
    camps = get_campaign_list(token)
    odps_writer = init_odps_writer(partition_dt)

    # 生成采集任务
    tasks = []
    for camp in camps:
        if not is_date_overlap(config_day, camp["s"], camp["e"]):
            continue

        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((token, camp, config_day, r, a, p, pos))

    print(f"🧩 总任务数：{len(tasks)}")

    # 启动写入线程
    write_thread = threading.Thread(target=write_worker, args=(odps_writer, partition_dt), daemon=True)
    write_thread.start()

    # 并发采集
    with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as executor:
        futures = [executor.submit(collect_task, *t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    # 等待写入完成
    write_finished = True
    write_thread.join()
    odps_writer.close()

    # 最终统计
    print("\n" + "=" * 90)
    print(f"✅ 任务完成！")
    print(f"📦 分区：dt={partition_dt}")
    print(f"📊 总采集：{total_collected} 条")
    print(f"📊 总写入：{total_written} 条")
    print("=" * 90)

if __name__ == "__main__":
    main()