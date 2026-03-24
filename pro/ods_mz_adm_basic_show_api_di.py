# -*- coding: utf-8 -*-
"""
秒针广告API采集 - 本地CSV最终终极版
功能：
1. 输出本地 CSV
2. 活动时间严格校验
3. 只存储【本行对应原始数据片段】，不存完整接口返回
4. 解析过程优化
5. 打印耗时、大小、采集条数
6. 防OOM、高并发、异常安全
"""
import json
import threading
import time
import traceback
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== 配置 =====================
CONFIG = {
    "local": {
        "output_file": "miaozhen_ad_report_output.csv",
        "batch_size": 2000,
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
        return ""
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


# ===================== 本地CSV写入 =====================
def init_local_writer(file_path):
    file = open(file_path, "w", encoding="utf-8-sig", newline="")
    writer = csv.writer(file)
    header = [
        "campaign_id", "start_date", "end_date", "date",
        "by_position", "by_region", "by_device", "by_audience", "platform",
        "s_version", "api_platform", "total_spot_num",
        "audience", "publisher_id", "spot_id", "region_id", "universe",
        "imp_acc", "clk_acc", "uim_acc", "ucl_acc",
        "imp_day", "clk_day", "uim_day", "ucl_day",
        "imp_avg_day", "clk_avg_day", "uim_avg_day", "ucl_avg_day",
        "imp_h00", "imp_h23", "clk_h00", "clk_h23",
        "request_params", "row_raw_data", "etl_time"
    ]
    writer.writerow(header)
    return file, writer


def write_worker(file_path):
    global total_written
    file, writer = init_local_writer(file_path)
    batch_size = CONFIG["local"]["batch_size"]

    while not (write_finished and write_queue.empty()):
        batch = []
        while len(batch) < batch_size and not write_queue.empty():
            batch.append(write_queue.get())

        if batch:
            start_time = time.time()
            writer.writerows(batch)
            file.flush()
            cost_time = round(time.time() - start_time, 3)
            total_written += len(batch)
            print(f"✅ 写入本地 | 批次 {len(batch)} 条 | 耗时 {cost_time}s | 累计 {total_written}")

    file.close()


# ===================== API 采集 =====================
def get_miaozhen_token():
    try:
        resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as e:
        print("=" * 80)
        print("❌ 获取Token失败！")
        print(f"🔗 URL：{CONFIG['api']['token_url']}")
        print(f"❌ 异常：{str(e)}")
        print("=" * 80)
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
        print(f"✅ 有效活动：{len(campaign_list)} 个")
        return campaign_list
    except Exception as e:
        print("=" * 80)
        print("❌ 获取活动列表失败！")
        print(f"🔗 URL：{CONFIG['api']['campaign_url']}")
        print(f"❌ 异常：{str(e)}")
        traceback.print_exc()
        print("=" * 80)
        raise


def collect_report_data(token, campaign, check_date, by_region, by_audience, platform, by_position):
    global total_collected, skip_campaign_count
    camp_id = campaign["campaign_id"]
    camp_start = campaign["start_date"]
    camp_end = campaign["end_date"]

    # 活动时间校验
    if not is_campaign_include_date(camp_start, camp_end, check_date):
        skip_campaign_count += 1
        return

    # 防OOM
    while write_queue.qsize() > 1800:
        time.sleep(0.05)

    # 请求参数
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
        # 请求接口
        api_start = time.time()
        resp = requests.get(CONFIG['api']['report_url'], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        api_cost = round(time.time() - api_start, 3)
        size_mb = round(len(resp.content) / 1024 / 1024, 4)

        data = resp.json()
        items = data.get("items", [])
        rows = []

        # ===================== 核心优化：只解析 & 只存储本行原始片段 =====================
        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})

            # 只存本行对应的原始数据（不是整个接口返回）
            row_raw = json.dumps(item, ensure_ascii=False)

            row = [
                safe_str(camp_id), safe_str(camp_start), safe_str(camp_end), safe_str(data.get("date")),
                safe_str(by_position), safe_str(by_region), "all", safe_str(by_audience), safe_str(platform),
                safe_str(data.get("s_version")), safe_str(data.get("platform")), safe_str(data.get("total_spot_num")),
                safe_str(attr.get("audience")), safe_str(attr.get("publisher_id")), safe_str(attr.get("spot_id")),
                safe_str(attr.get("region_id")), safe_str(attr.get("universe")),
                safe_str(metric.get("imp_acc")), safe_str(metric.get("clk_acc")), safe_str(metric.get("uim_acc")),
                safe_str(metric.get("ucl_acc")),
                safe_str(metric.get("imp_day")), safe_str(metric.get("clk_day")), safe_str(metric.get("uim_day")),
                safe_str(metric.get("ucl_day")),
                safe_str(metric.get("imp_avg_day")), safe_str(metric.get("clk_avg_day")),
                safe_str(metric.get("uim_avg_day")), safe_str(metric.get("ucl_avg_day")),
                safe_str(metric.get("imp_h00")), safe_str(metric.get("imp_h23")),
                safe_str(metric.get("clk_h00")), safe_str(metric.get("clk_h23")),
                json.dumps(params, ensure_ascii=False),
                row_raw,  # 只存本行原始片段 ✅
                get_etl_datetime()
            ]
            rows.append(row)

        # 写入队列
        total_collected += len(rows)
        for r in rows:
            write_queue.put(r)

        print(
            f"📥 采集 {camp_id} | {by_position}-{by_region}-{by_audience}-{platform} | 条数 {len(rows)} | 耗时 {api_cost}s | 大小 {size_mb}MB")

    except Exception as e:
        print(f"❌ 失败 {camp_id} | {type(e).__name__}")


# ===================== 主流程 =====================
def main():
    global write_finished
    print("=" * 80)
    print("🚀 秒针API采集 - 本地CSV最终版（只存本行原始片段）")
    print("=" * 80)

    dt = CONFIG["local"]["dt"]
    check_date = format_date(dt)
    token = get_miaozhen_token()
    campaign_list = get_valid_campaign_list(token)

    # 生成任务
    tasks = []
    for camp in campaign_list:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((token, camp, check_date, r, a, p, pos))

    print(f"🧩 总任务数：{len(tasks)}")

    # 写入线程
    write_thread = threading.Thread(target=write_worker, args=(CONFIG["local"]["output_file"],), daemon=True)
    write_thread.start()

    # 并发采集
    with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as executor:
        futures = [executor.submit(collect_report_data, *t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    # 结束
    write_finished = True
    write_thread.join()

    print("\n" + "=" * 50)
    print("✅ 任务完成！")
    print(f"文件：{CONFIG['local']['output_file']}")
    print(f"总跳过：{skip_campaign_count}")
    print(f"总采集：{total_collected}")
    print(f"总写入：{total_written}")
    print("=" * 50)


if __name__ == "__main__":
    main()