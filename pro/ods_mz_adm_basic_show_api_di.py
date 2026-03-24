# -*- coding: utf-8 -*-
"""
秒针广告API采集 - 最终版（严格匹配report_url数据结构）
API并行：10
写入：MaxCompute 批量覆盖写入
功能：metrics为空 → 自动跳过不入库
"""
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
import requests
import urllib3
from odps import ODPS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
odps_client = ODPS()
ODPS_PROJECT = odps_client.project

# ===================== 配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",
        "dt": "20260301"
    },
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
    }
}

# 全局数据
all_data = []
total_collected = 0


# ===================== 工具方法 =====================
def get_log():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_str(val):
    # 专门处理API返回的 "-" / null / 空值
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


# ===================== 拉取报表数据（适配你的数据结构） =====================
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
        raw_response = resp.text

        rows = []
        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})

            # 空指标直接跳过
            if not metric:
                continue

            # 【完全匹配你提供的API结构】
            row = [
                safe_str(cid),
                safe_str(camp["start_date"]),
                safe_str(camp["end_date"]),
                safe_str(data.get("date")),
                safe_str(pos),
                safe_str(reg),
                "all",
                safe_str(aud),
                safe_str(plt),
                safe_str(data.get("version")),
                safe_str(data.get("platform")),
                safe_str(data.get("total_spot_num")),

                # attributes 严格匹配
                safe_str(attr.get("audience")),
                safe_str(attr.get("target_id")),
                safe_str(attr.get("publisher_id")),
                safe_str(attr.get("spot_id")),
                safe_str(attr.get("keyword_id", "")),  # 可能不存在
                safe_str(attr.get("region_id")),
                safe_str(attr.get("universe")),

                # metrics 核心指标（你提供的结构体）
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

                # 小时字段（你提供的结构体只有这6个）
                safe_str(metric.get("imp_h00")),
                safe_str(metric.get("imp_h23")),
                safe_str(metric.get("clk_h00")),
                safe_str(metric.get("clk_h23")),

                # 扩展小时字段（不存在则为空）
                safe_str(metric.get("imp_h01", "")),
                safe_str(metric.get("imp_h02", "")),
                safe_str(metric.get("clk_h01", "")),
                safe_str(metric.get("clk_h02", "")),

                json.dumps(params, ensure_ascii=False),
                raw_response,
                get_log()
            ]
            rows.append(row)

        all_data.extend(rows)
        total_collected += len(rows)
        print(f"[{get_log()}] 📥 {cid} | {len(rows)} 条 | 总计：{total_collected}")

    except Exception as e:
        print(f"[{get_log()}] ❌ {cid} 失败：{str(e)[:100]}")
        return


# ===================== MaxCompute 批量覆盖写入 =====================
def write_all_to_odps(dt, data_list):
    if not data_list:
        print("❌ 无有效数据可写入")
        return

    table = odps_client.get_table(CONFIG["odps"]["table_name"])
    partition = f"dt='{dt}'"

    # 覆盖写入：先删分区
    try:
        odps_client.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION IF EXISTS ({partition})")
    except Exception as e:
        print(f"[{get_log()}] ⚠️ 删除分区异常：{e}")

    print(f"[{get_log()}] ✍️ 开始写入 {len(data_list)} 条数据")
    start = time.time()

    with table.open_writer(partition=partition, create_partition=True) as writer:
        writer.write(data_list)

    print(f"[{get_log()}] ✅ 写入完成！耗时 {round(time.time() - start, 2)}s")


# ===================== 主函数 =====================
def main():
    dt = CONFIG["odps"]["dt"]
    check_dt = format_date(dt)
    print(f"[{get_log()}] 🚀 开始采集（并行10 | 空metrics自动跳过）")

    token = get_token()
    camps = get_campaigns(token)
    if not camps:
        print("❌ 无有效活动")
        return

    # 构造任务
    tasks = []
    for c in camps:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((c, r, a, p, pos))

    # 并行采集
    with ThreadPoolExecutor(CONFIG["api"]["api_workers"]) as pool:
        futures = [pool.submit(fetch_task, t, token, check_dt) for t in tasks]
        wait(futures)

    # 写入MaxCompute
    write_all_to_odps(dt, all_data)

    print("\n" + "=" * 50)
    print(f"[{get_log()}] 🎉 任务完成")
    print(f"总有效数据：{total_collected} 条")
    print(f"成功入库：{len(all_data)} 条")
    print("=" * 50)


if __name__ == "__main__":
    main()