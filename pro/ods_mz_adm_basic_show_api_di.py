# -*- coding: utf-8 -*-
"""
秒针广告API数据采集 - 单线程最终版
功能：
1. 单线程串行执行（无任何多线程）
2. 每批次写入 20000 条
3. 写入MaxCompute分区表
4. 日志：时间 + API耗时 + 写入耗时
5. 仅存储本行解析数据
6. 活动时间校验
"""
import json
import time
from datetime import datetime
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
        "batch_size": 20000,  # 每20000条写入一次
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
        "timeout": 60
    }
}


# ===================== 工具方法 =====================
def get_log_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


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

    try:
        odps.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION IF EXISTS ({partition_spec})")
    except:
        pass

    return table.open_writer(partition=partition_spec, create_partition=True)


def write_batch(writer, batch_data, dt):
    if not batch_data:
        return

    start = time.time()
    writer.write(batch_data)
    cost = round(time.time() - start, 3)
    print(f"[{get_log_time()}] ✅ 写入分区 {dt} | 批次 {len(batch_data)} 条 | 写入耗时 {cost}s")


# ===================== API 采集 =====================
def get_miaozhen_token():
    start = time.time()
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    cost = round(time.time() - start, 3)
    print(f"[{get_log_time()}] 🔑 获取Token成功 | 耗时 {cost}s")
    return resp.json()["access_token"]


def get_valid_campaign_list(token):
    start = time.time()
    resp = requests.get(f"{CONFIG['api']['campaign_url']}?access_token={token}", timeout=60, verify=False)
    resp.raise_for_status()
    cost = round(time.time() - start, 3)
    campaign_list = []
    for camp in resp.json():
        camp_id = camp.get("campaign_id")
        start_date = camp.get("start_date")
        end_date = camp.get("end_date")
        if camp_id and start_date and end_date:
            campaign_list.append({"campaign_id": str(camp_id), "start_date": start_date, "end_date": end_date})
    print(f"[{get_log_time()}] 📋 获取活动列表 {len(campaign_list)} 个 | 耗时 {cost}s")
    return campaign_list


# ===================== 主流程（纯单线程） =====================
def main():
    dt = CONFIG["odps"]["dt"]
    check_date = format_date(dt)
    print(f"[{get_log_time()}] 🚀 开始采集 | 分区 dt={dt} | 单线程模式 | 批次 20000")

    token = get_miaozhen_token()
    campaign_list = get_valid_campaign_list(token)
    writer = init_odps_writer(dt)

    batch_size = CONFIG["odps"]["batch_size"]
    cache = []
    total_collect = 0
    total_skip = 0

    # 遍历所有任务（串行）
    for camp in campaign_list:
        camp_id = camp["campaign_id"]
        camp_start = camp["start_date"]
        camp_end = camp["end_date"]

        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:

                        if not is_campaign_include_date(camp_start, camp_end, check_date):
                            total_skip += 1
                            continue

                        params = {
                            "access_token": token,
                            "campaign_id": camp_id,
                            "date": check_date,
                            "metrics": "all",
                            "by_region": r,
                            "by_audience": a,
                            "platform": p,
                            "by_position": pos
                        }

                        try:
                            api_start = time.time()
                            resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
                            resp.raise_for_status()
                            api_cost = round(time.time() - api_start, 3)
                            data = resp.json()
                            items = data.get("items", [])

                            print(
                                f"[{get_log_time()}] 📥 {camp_id} | {pos}-{r}-{a}-{p} | {len(items)} 条 | API耗时 {api_cost}s")

                            for item in items:
                                attr = item.get("attributes", {})
                                metric = item.get("metrics", {})
                                row_raw = json.dumps(item, ensure_ascii=False)

                                row = [
                                    safe_str(camp_id), safe_str(camp_start), safe_str(camp_end),
                                    safe_str(data.get("date")),
                                    safe_str(pos), safe_str(r), "all", safe_str(a), safe_str(p),
                                    safe_str(data.get("s_version")), safe_str(data.get("platform")),
                                    safe_str(data.get("total_spot_num")),
                                    safe_str(attr.get("audience")), None, safe_str(attr.get("publisher_id")),
                                    safe_str(attr.get("spot_id")),
                                    None, safe_str(attr.get("region_id")), safe_str(attr.get("universe")),
                                    safe_str(metric.get("imp_acc")), safe_str(metric.get("clk_acc")),
                                    safe_str(metric.get("uim_acc")), safe_str(metric.get("ucl_acc")),
                                    safe_str(metric.get("imp_day")), safe_str(metric.get("clk_day")),
                                    safe_str(metric.get("uim_day")), safe_str(metric.get("ucl_day")),
                                    safe_str(metric.get("imp_avg_day")), safe_str(metric.get("clk_avg_day")),
                                    safe_str(metric.get("uim_avg_day")), safe_str(metric.get("ucl_avg_day")),
                                    safe_str(metric.get("imp_h00")), safe_str(metric.get("imp_h23")),
                                    safe_str(metric.get("clk_h00")), safe_str(metric.get("clk_h23")),
                                    json.dumps(params, ensure_ascii=False),
                                    row_raw,
                                    get_etl_datetime()
                                ]

                                cache.append(row)
                                total_collect += 1

                                # 满20000条写入一次
                                if len(cache) >= batch_size:
                                    write_batch(writer, cache, dt)
                                    cache = []

                        except Exception:
                            continue

    # 最后剩余不足20000条全部写入
    if cache:
        write_batch(writer, cache, dt)

    writer.close()
    print(f"[{get_log_time()}] ✅ 任务完成")
    print(f"[{get_log_time()}] 📊 总跳过：{total_skip} | 总采集：{total_collect} | 总写入：{total_collect}")


if __name__ == "__main__":
    main()