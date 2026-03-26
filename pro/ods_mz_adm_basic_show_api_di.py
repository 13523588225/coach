# -*- coding: utf-8 -*-
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

# 关闭SSL不安全请求警告
urllib3.disable_warnings(InsecureRequestWarning)

# ===================== 核心配置 =====================
ODPS_PROJECT = "coach_marketing_hub_dev"
DT = args['dt']
PARALLEL_CONFIG = {
    "batch_size": 20000
}

# ===================== 业务配置 =====================
CONFIG = {
    "odps_table": "ods_mz_adm_basic_show_api_di",
    "report_params": {
        "by_region_list": ["level0", "level1", "level2"],
        "by_audience_list": ["overall", "stable", "target"],
        "platform_list": ["pc", "pm", "mb"],
        "by_position_list": ["campaign", "publisher", "spot"]
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
    }
}

all_data = []
total_collected = 0

# ===================== 工具方法 =====================

def get_log():
    """获取标准化日志时间"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_str(val):
    """安全字符串转换，处理空值与特殊字符"""
    if val is None or val == "" or val == "-" or str(val).lower() in ["null", "undefined"]:
        return ""
    return str(val).replace("\n", " ").replace("\r", "")

def format_date(dt):
    """日期格式转换 yyyyMMdd → yyyy-MM-dd"""
    return datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")

def is_in_range(start, end, check):
    """判断报表日期是否在活动时间范围内"""
    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        c = datetime.strptime(check, "%Y-%m-%d")
        return s <= c <= e
    except:
        return False

# ===================== MaxCompute 写入 =====================

def write_to_odps_partition(table_name: str, data: List[List]):
    """
    批量写入MaxCompute分区表
    自动删除旧分区 + 分批次提交
    """
    if not data:
        print(f"⚠️ 分区{DT}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt={DT}"

    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size

    try:
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 已清空旧分区：{DT}")

        print(f"📊 总数据量 {total_count} 条，分 {batch_num} 批次写入")
        total_cost = 0

        for i in range(batch_num):
            t0 = time.time()
            batch = data[i*batch_size : (i+1)*batch_size]
            with table.open_writer(partition=partition_spec, create_partition=True) as w:
                w.write(batch)
            cost = round(time.time() - t0, 2)
            total_cost += cost
            print(f"💾 批次 {i+1}/{batch_num} 完成 | 条数={len(batch)} | 耗时={cost}s")

        print(f"✅ 分区 {DT} 全部写入完成，总耗时 {round(total_cost,2)}s")

    except errors.ODPSError as e:
        raise Exception(f"写入失败：{str(e)}")

# ===================== API 获取Token =====================

def get_token():
    """获取秒针API访问凭证"""
    t0 = time.time()
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"[{get_log()}] 🔑 获取TOKEN成功，耗时 {round(time.time()-t0,2)}s")
    return token

# ===================== 获取活动列表 =====================

def get_campaigns(token):
    """获取活动基础信息（ID、起止日期）"""
    resp = requests.get(f"{CONFIG['api']['campaign_url']}?access_token={token}", timeout=60, verify=False)
    resp.raise_for_status()
    campaigns = []
    for item in resp.json():
        cid = item.get("campaign_id")
        sdt = item.get("start_date")
        edt = item.get("end_date")
        if cid and sdt and edt:
            campaigns.append({"campaign_id": str(cid), "start_date": sdt, "end_date": edt})
    print(f"[{get_log()}] 📋 有效活动数：{len(campaigns)}")
    return campaigns

# ===================== 拉取报表数据 =====================

def fetch_task(task, token, dt):
    """多线程执行：拉取单维度报表数据"""
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

        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
            if not metric:
                continue

            # ===================== 字段 100% 匹配你的表结构 =====================
            row = [
                safe_str(cid),                                # campaign_id
                safe_str(camp["start_date"]),                 # campaign_start_date
                safe_str(camp["end_date"]),                   # campaign_end_date
                safe_str(data.get("date")),                   # report_day_date
                safe_str(pos),                                # by_position
                safe_str(reg),                                # by_region
                "all",                                        # metrics
                safe_str(aud),                                # by_audience
                safe_str(plt),                                # platform
                safe_str(data.get("version")),                # s_version
                safe_str(data.get("platform")),               # platform_resp
                safe_str(data.get("total_spot_num")),         # total_spot_num
                safe_str(attr.get("audience")),               # audience
                safe_str(attr.get("target_id")),              # target_id
                safe_str(attr.get("publisher_id")),           # publisher_id
                safe_str(attr.get("spot_id")),                # spot_id
                safe_str(attr.get("keyword_id")),             # keyword_id
                safe_str(attr.get("region_id")),              # region_id
                safe_str(attr.get("universe")),               # universe
                safe_str(metric.get("imp_acc")),              # imp_acc
                safe_str(metric.get("clk_acc")),              # clk_acc
                safe_str(metric.get("uim_acc")),              # uim_acc
                safe_str(metric.get("ucl_acc")),              # ucl_acc
                safe_str(metric.get("imp_day")),              # imp_day
                safe_str(metric.get("clk_day")),              # clk_day
                safe_str(metric.get("uim_day")),              # uim_day
                safe_str(metric.get("ucl_day")),              # ucl_day
                safe_str(metric.get("imp_avg_day")),          # imp_avg_day
                safe_str(metric.get("clk_avg_day")),          # clk_avg_day
                safe_str(metric.get("uim_avg_day")),          # uim_avg_day
                safe_str(metric.get("ucl_avg_day")),          # ucl_avg_day
                safe_str(metric.get("imp_h00")),              # imp_acc_h00
                safe_str(metric.get("imp_h23")),              # imp_acc_h23
                safe_str(metric.get("clk_h00")),              # clk_acc_h00
                safe_str(metric.get("clk_h23")),              # clk_acc_h23
                json.dumps(params, ensure_ascii=False),       # request_params
                "",                                           # pre_parse_raw_text（置空）
                get_log()                                     # etl_datetime
            ]

            all_data.append(row)
            total_collected += 1

        print(f"[{get_log()}] 📥 活动 {cid} 完成 | 累计 {total_collected} 条")

    except Exception as e:
        print(f"[{get_log()}] ❌ 活动 {cid} 失败：{str(e)[:100]}")

# ===================== 主流程 =====================

def main():
    run_date = format_date(DT)
    print(f"[{get_log()}] 🚀 开始采集 | 分区 dt={DT}")

    token = get_token()
    campaigns = get_campaigns(token)
    if not campaigns:
        print("❌ 无有效活动")
        return

    # 构建任务
    tasks = []
    for c in campaigns:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((c, r, a, p, pos))

    # 多线程采集
    with ThreadPoolExecutor(CONFIG["api"]["api_workers"]) as pool:
        futures = [pool.submit(fetch_task, t, token, run_date) for t in tasks]
        wait(futures)

    # 写入MaxCompute
    write_to_odps_partition(CONFIG["odps_table"], all_data)

    print("\n" + "="*60)
    print(f"🎉 任务完成")
    print(f"采集条数：{total_collected}")
    print(f"项目：{ODPS_PROJECT}")
    print(f"表名：{CONFIG['odps_table']}")
    print(f"分区：dt={DT}")
    print("="*60)

if __name__ == "__main__":
    main()
