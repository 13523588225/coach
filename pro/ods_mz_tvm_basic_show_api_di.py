# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
import threading
from datetime import datetime
from typing import Dict, List, Tuple, Any
from odps import ODPS

# 关键：初始化ODPS客户端，自动获取项目名
ODPS_CLIENT = ODPS()  # 自动读取环境变量中的ODPS配置
ODPS_PROJECT = ODPS_CLIENT.project  # 自动获取当前项目名
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== 极简配置（移除手动填写ODPS项目名） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    "odps_table": "ods_mz_tvm_basic_show_api_di",
    "process_dt": "20260301",
    "mz_auth": ("Coach_api", "Coachapi2026"),
    "combo_workers": 10,
    "campaign_workers": 10,
    "global_sem": 50,
    "flush_threshold": 50000
}

HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]
GLOBAL_SEM = threading.Semaphore(CONFIG["global_sem"])
TOTAL_ROWS = 0
BATCH_STAT = []  # 批次统计：(批次号, 数据量, 耗时)


# ===================== 工具函数 =====================
def date_8to10(dt8: str) -> str:
    return f"{dt8[:4]}-{dt8[4:6]}-{dt8[6:8]}"


def clean_val(val: Any, is_num: bool = False) -> Any:
    if val is None or val == "" or val == "null":
        return 0 if is_num else ""
    if is_num:
        try:
            return int(val) if str(val).isdigit() else float(val)
        except:
            return 0
    return str(val).strip()


# ===================== 请求函数 =====================
def req_get(url: str, params: Dict = None) -> Dict:
    with GLOBAL_SEM:
        resp = requests.get(url, params=params, timeout=30, verify=False)
        resp.raise_for_status()
        time.sleep(0.01)
        return resp.json()


def req_post(url: str, data: Dict = None) -> Dict:
    resp = requests.post(url, data=data, timeout=30, verify=False)
    resp.raise_for_status()
    return resp.json()


# ===================== 业务逻辑 =====================
def get_token() -> str:
    res = req_post(
        "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
        data={"username": CONFIG["mz_auth"][0], "password": CONFIG["mz_auth"][1]}
    )
    if res.get("error_code") != 0 or not res.get("result", {}).get("access_token"):
        raise Exception(f"Token失败：{res.get('error_message', '未知错误')}")
    return res["result"]["access_token"]


def get_campaigns(token: str) -> List[Dict]:
    res = req_get(
        "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
        params={"access_token": token}
    )
    campaigns = [
        {"id": clean_val(c.get("campaign_id")), "start": clean_val(c.get("start_time")),
         "end": clean_val(c.get("end_time"))}
        for c in res.get("result", {}).get("campaigns", [])
        if isinstance(c, dict) and c.get("campaign_id")
    ]
    if not campaigns:
        raise Exception("活动列表为空")
    return campaigns


def parse_campaign(token: str, camp: Dict, dt: str, combo: Tuple) -> List[List[Any]]:
    by_region, by_aud, platform, by_pos = combo
    try:
        if not (datetime.strptime(camp["start"], "%Y-%m-%d") <= datetime.strptime(dt, "%Y%m%d") <= datetime.strptime(
                camp["end"], "%Y-%m-%d")):
            return []
    except:
        return []

    params = {
        "campaign_id": camp["id"],
        "date": date_8to10(dt),
        "metrics": "all",
        "by_region": by_region,
        "by_audience": by_aud,
        "platform": platform,
        "by_position": by_pos,
        "access_token": token
    }
    res = req_get("https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show", params=params)

    if res.get("error_code") != 0 or not res.get("result", {}).get("items"):
        raise Exception(f"报表异常：活动{camp['id']}")

    items = res["result"]["items"]
    etl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        attr = item.get("attributes", {})
        metrics = item.get("metrics", {})

        row = [
            clean_val(params["campaign_id"]), clean_val(params["date"]), "all",
            clean_val(by_region), clean_val(by_aud), clean_val(platform), clean_val(by_pos),
            clean_val(res["result"].get("campaignId")), clean_val(camp["start"]), clean_val(camp["end"]),
            clean_val(res["result"].get("date")), clean_val(res["result"].get("version"), True),
            clean_val(attr.get("publisher_id")), clean_val(attr.get("spot_id")), clean_val(attr.get("spot_id_str")),
            clean_val(attr.get("audience")), clean_val(attr.get("universe")), clean_val(attr.get("region_id")),
            clean_val(metrics.get("imp_acc"), True), clean_val(metrics.get("clk_acc"), True),
            clean_val(metrics.get("uim_acc"), True), clean_val(metrics.get("ucl_acc"), True),
            clean_val(metrics.get("imp_day"), True), clean_val(metrics.get("clk_day"), True),
            clean_val(metrics.get("uim_day"), True), clean_val(metrics.get("ucl_day"), True),
            clean_val(metrics.get("imp_avg_day"), True), clean_val(metrics.get("clk_avg_day"), True),
            clean_val(metrics.get("uim_avg_day"), True), clean_val(metrics.get("ucl_avg_day"), True),
            *[clean_val(metrics.get(f"imp_{h}"), True) for h in HOUR_FIELDS],
            *[clean_val(metrics.get(f"clk_{h}"), True) for h in HOUR_FIELDS],
            json.dumps({"attr": attr, "metrics": metrics}, ensure_ascii=False), etl_time
        ]
        rows.append(row)
    return rows


# 批次写入+时间统计
def write_odps(data: List[List[Any]], dt: str, batch_num: int):
    if not data:
        return
    batch_start = time.time()
    table = ODPS_CLIENT.get_table(CONFIG["odps_table"])  # 使用全局ODPS客户端
    with table.open_writer(partition=f"dt='{dt}'", create_partition=True) as writer:
        writer.write(data)
    batch_cost = round(time.time() - batch_start, 2)
    BATCH_STAT.append((batch_num, len(data), batch_cost))
    print(f"批次{batch_num}完成 | 数据量：{len(data)}条 | 耗时：{batch_cost}秒")


# ===================== 主流程 =====================
def main():
    global TOTAL_ROWS
    task_start = time.time()
    dt = CONFIG["process_dt"]
    print(f"===== 任务启动：{dt} | {datetime.now()} =====")
    print(f"自动识别ODPS项目：{ODPS_PROJECT}")  # 打印自动获取的项目名

    try:
        # 1. 初始化
        token = get_token()
        campaigns = get_campaigns(token)
        print(f"Token成功 | 活动数：{len(campaigns)}")

        # 2. 生成参数组合
        combos = [
            (r, a, p, pos)
            for r in ["level0", "level1", "level2"]
            for a in ["overall", "stable", "target"]
            for p in ["pc", "pm", "mb"]
            for pos in ["campaign", "publisher", "spot", "keyword"]
        ]

        # 3. 双层并发解析+增量写入
        cache = []
        batch_num = 0
        with ThreadPoolExecutor(max_workers=CONFIG["combo_workers"]) as combo_exec:
            futures = [combo_exec.submit(parse_combo, token, campaigns, dt, combo) for combo in combos]
            for future in as_completed(futures):
                combo_data = future.result()
                cache.extend(combo_data)
                TOTAL_ROWS += len(combo_data)

                # 增量写入
                if len(cache) >= CONFIG["flush_threshold"]:
                    batch_num += 1
                    write_odps(cache, dt, batch_num)
                    cache = []
                    gc.collect()

        # 4. 剩余数据写入
        if cache:
            batch_num += 1
            write_odps(cache, dt, batch_num)

        # 5. 数据量校验
        count_sql = f"SELECT COUNT(*) FROM {CONFIG['odps_table']} WHERE dt='{dt}'"
        db_count = ODPS_CLIENT.execute_sql(count_sql).open_reader().next()[0]
        if db_count != TOTAL_ROWS:
            raise Exception(f"数据量不一致：解析{TOTAL_ROWS}条 | 写入{db_count}条")

        # 6. 汇总统计
        task_cost = round(time.time() - task_start, 2)
        print(f"\n===== 任务完成汇总 =====")
        print(f"ODPS项目：{ODPS_PROJECT}")
        print(f"目标表：{CONFIG['odps_table']}")
        print(f"总解析行数：{TOTAL_ROWS}")
        print(f"总写入批次：{batch_num}")
        print(f"任务总耗时：{task_cost}秒")
        print(f"平均速率：{round(TOTAL_ROWS / task_cost, 2)}条/秒")
        if BATCH_STAT:
            print(f"\n各批次详情：")
            for batch in BATCH_STAT:
                print(f"  批次{batch[0]}：{batch[1]}条 | {batch[2]}秒 | {round(batch[1] / batch[2], 2)}条/秒")

    except Exception as e:
        task_cost = round(time.time() - task_start, 2)
        print(f"\n===== 任务失败 =====")
        print(f"ODPS项目：{ODPS_PROJECT}")
        print(f"失败原因：{str(e)}")
        print(f"任务耗时：{task_cost}秒")
        raise


def parse_combo(token: str, campaigns: List[Dict], dt: str, combo: Tuple) -> List[List[Any]]:
    combo_data = []
    with ThreadPoolExecutor(max_workers=CONFIG["campaign_workers"]) as camp_exec:
        futures = [camp_exec.submit(parse_campaign, token, camp, dt, combo) for camp in campaigns]
        for future in as_completed(futures):
            combo_data.extend(future.result())
    return combo_data


if __name__ == "__main__":
    # 无需手动配置ODPS项目名，直接运行
    main()