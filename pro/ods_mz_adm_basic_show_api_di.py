# -*- coding: utf-8 -*-
"""
秒针广告API数据采集脚本
功能：
1. 高并发采集报表数据
2. 边采集边写入MaxCompute，避免内存溢出
3. 仅当采集日期在活动有效期内，才执行report_url请求
4. 默认分区：dt=20260301
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

# 关闭SSL不安全警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 自动获取ODPS项目名称
ODPS_PROJECT = ODPS().project

# ===================== 全局配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",
        "batch_size": 20000,
        "write_workers": 10,
        "dt": "20260301"  # 已直接固定分区
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
        "interval": 0.01,  # 已改为 0.01
        "api_workers": 20
    }
}

# ===================== 全局变量（边采边写） =====================
write_queue = Queue(maxsize=100000)
write_finished = False
total_written = 0
total_collected = 0

# ===================== 工具方法 =====================
def safe_str(val):
    """安全转换字符串，空值/异常值返回None"""
    if val is None or val == "" or val == "-" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_datetime():
    """获取采集时间：yyyy-MM-dd HH:mm:ss"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_date(date_ymd):
    """yyyyMMdd → yyyy-MM-dd"""
    return datetime.strptime(date_ymd, "%Y%m%d").strftime("%Y-%m-%d")


def is_campaign_include_date(camp_start_date, camp_end_date, check_date):
    """
    核心校验：判断采集日期是否在活动的起止日期范围内
    :return: True=执行采集 False=跳过
    """
    try:
        camp_start = datetime.strptime(camp_start_date, "%Y-%m-%d")
        camp_end = datetime.strptime(camp_end_date, "%Y-%m-%d")
        check = datetime.strptime(check_date, "%Y-%m-%d")
        return camp_start <= check <= camp_end
    except Exception:
        return False

# ===================== MaxCompute 写入模块 =====================
def init_odps_writer(partition_dt):
    """初始化ODPS写入器，写入前清空历史分区"""
    odps = ODPS(project=ODPS_PROJECT)
    options.tunnel.use_instance_tunnel = True

    table = odps.get_table(CONFIG["odps"]["table_name"])
    partition_spec = f"dt='{partition_dt}'"

    # 清空旧数据，保证幂等
    if table.exist_partition(partition_spec):
        odps.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION ({partition_spec})")

    return table.open_writer(partition=partition_spec, create_partition=True)


def write_worker(odps_writer, partition_dt):
    """
    写入工作线程：从队列消费数据，批量写入MaxCompute
    实现边采边写，防止内存溢出
    """
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
            print(f"✅ 写入分区 {partition_dt} | 批次 {len(batch)} 条 | 写入耗时 {cost_time}s | 累计写入 {total_written} 条")


# ===================== API 采集模块 =====================
def get_miaozhen_token():
    """获取秒针API访问token"""
    resp = requests.post(
        CONFIG["api"]["token_url"],
        data=CONFIG["api"]["auth"],
        timeout=60,
        verify=False
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_valid_campaign_list(token):
    """获取有效活动列表（含ID、开始、结束日期）"""
    resp = requests.get(
        f"{CONFIG['api']['campaign_url']}?access_token={token}",
        timeout=60,
        verify=False
    )
    resp.raise_for_status()
    campaign_list = []

    for camp in resp.json():
        camp_id = camp.get("campaign_id")
        start_date = camp.get("start_date")
        end_date = camp.get("end_date")

        if camp_id and start_date and end_date:
            campaign_list.append({
                "campaign_id": str(camp_id),
                "start_date": start_date,
                "end_date": end_date
            })

    print(f"✅ 成功获取 {len(campaign_list)} 个有效活动")
    return campaign_list


def collect_report_data(token, campaign, check_date, by_region, by_audience, platform, by_position):
    """
    采集任务：日期校验通过 → 请求report_url → 数据入队列
    """
    global total_collected
    camp_id = campaign["campaign_id"]
    camp_start = campaign["start_date"]
    camp_end = campaign["end_date"]

    # 日期校验：不在活动有效期直接跳过，不请求API
    if not is_campaign_include_date(camp_start, camp_end, check_date):
        print(f"ℹ️ 活动 {camp_id}：日期 {check_date} 不在有效期内，跳过report_url")
        return

    try:
        params = {
            "access_token": token,
            "campaign_id": camp_id,
            "date": check_date,
            "metrics": CONFIG["report_params"]["metrics"],
            "by_region": by_region,
            "by_audience": by_audience,
            "platform": platform,
            "by_position": by_position
        }

        # 请求接口计时
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
                safe_str(camp_id),
                safe_str(camp_start),
                safe_str(camp_end),
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

        print(f"📥 采集成功 | 活动 {camp_id} | 条数 {len(rows)} | API耗时 {api_cost}s")

    except Exception as e:
        print(f"❌ 采集失败 | 活动 {camp_id} | 错误：{str(e)}")


# ===================== 主流程 =====================
def main():
    global write_finished

    print("=" * 90)
    print("🚀 秒针API高并发采集（活动日期校验版）")
    print("📦 固定分区 dt=20260301")
    print("✅ 仅在活动有效期内才请求report_url")
    print("=" * 90)

    # 固定日期
    DEFAULT_DT = CONFIG["odps"]["dt"]
    check_date = format_date(DEFAULT_DT)
    partition_dt = DEFAULT_DT

    # 初始化
    token = get_miaozhen_token()
    campaign_list = get_valid_campaign_list(token)
    odps_writer = init_odps_writer(partition_dt)

    # 生成采集任务
    tasks = []
    for camp in campaign_list:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((token, camp, check_date, r, a, p, pos))

    print(f"\n🧩 生成总任务数：{len(tasks)}")

    # 启动写入线程
    write_thread = threading.Thread(target=write_worker, args=(odps_writer, partition_dt), daemon=True)
    write_thread.start()

    # 并发采集
    with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as executor:
        futures = [executor.submit(collect_report_data, *task) for task in tasks]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass

    # 等待写入完成
    write_finished = True
    write_thread.join()
    odps_writer.close()

    # 统计
    print("\n" + "=" * 90)
    print("✅ 任务全部执行完成")
    print(f"📦 目标分区：dt={partition_dt}")
    print(f"📊 总采集条数：{total_collected}")
    print(f"📊 总写入条数：{total_written}")
    print("=" * 90)


if __name__ == "__main__":
    main()