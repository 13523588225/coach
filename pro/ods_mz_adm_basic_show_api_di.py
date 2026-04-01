# -*- coding: utf-8 -*-
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Dict
import requests
from requests import Request
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from odps import ODPS
from odps import errors

# 关闭SSL不安全请求警告
urllib3.disable_warnings(InsecureRequestWarning)

# ===================== 核心配置 =====================
ODPS_PROJECT = ODPS().project
DT = args['dt']
PARALLEL_CONFIG = {
    "batch_size": 20000
}

# ===================== 报表参数配置 =====================
REPORT_PARAMS = {
    "metrics": "all",
    "by_region": ["level0", "level1", "level2"],
    "by_audience": ["overall", "stable", "target"],
    "platform": ["pc", "pm", "mb"],
    "by_position": ["campaign", "publisher", "spot"]
}

# ===================== 业务配置 =====================
CONFIG = {
    "odps_table": "ods_mz_adm_basic_show_api_di",
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "report_url": "https://api.cn.miaozhen.com/admonitor/v1/reports/basic/show",
        "auth": {
            "grant_type": "password",
            "username": "",
            "password": "",
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


get_etl_time = get_log


def safe_str(val):
    """安全字符串转换，处理空值与特殊字符"""
    if val is None or val == "" or val == "-" or str(val).lower() in ["null", "undefined"]:
        return ""
    return str(val).replace("\n", " ").replace("\r", "")


to_string = safe_str


def format_date(dt):
    """日期格式转换 yyyyMMdd → yyyy-MM-dd"""
    return datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")


# ===================== 从MaxCompute查询API账号密码 =====================
def get_adm_api_credentials():
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'ADM' limit 1 \
          """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_log()}] 🔐 成功从数仓获取ADM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"❌ 查询账号密码失败：{str(e)}")


# ===================== MaxCompute 写入 =====================
def write_to_odps_partition(table_name: str, data: List[List]):
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
            o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"✅ 已清空旧分区：{DT}")

        print(f"📊 总数据量 {total_count} 条，分 {batch_num} 批次写入")
        total_cost = 0
        for i in range(batch_num):
            t0 = time.time()
            batch = data[i * batch_size: (i + 1) * batch_size]
            with table.open_writer(partition=partition_spec, create_partition=True) as w:
                w.write(batch)
            cost = round(time.time() - t0, 2)
            total_cost += cost
            print(f"💾 批次 {i + 1}/{batch_num} 完成 | 条数={len(batch)} | 耗时={cost}s")
        print(f"✅ 分区 {DT} 全部写入完成，总耗时 {round(total_cost, 2)}s")

    except errors.ODPSError as e:
        raise Exception(f"写入失败：{str(e)}")


# ===================== API 获取Token =====================
def get_token():
    t0 = time.time()
    resp = requests.post(CONFIG["api"]["token_url"], data=CONFIG["api"]["auth"], timeout=60, verify=False)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"[{get_log()}] 🔑 获取TOKEN成功，耗时 {round(time.time() - t0, 2)}s")
    return token


# ===================== 获取活动列表（极简版 - 你指定的代码） =====================
def get_campaign_list() -> List[Dict]:
    try:
        o = ODPS(project=ODPS_PROJECT)
        sql = f"""
              select campaign_id, start_date, end_date
              from ods_mz_adm_campaigns_list_api_df 
              where dt = MAX_PT('ods_mz_adm_campaigns_list_api_df')
              and '{DT}' between replace(start_date, '-', '') and replace(end_date, '-', '')
              """
        print(f"[{get_etl_time()}] 📝 执行活动SQL：{sql}")

        valid_campaigns = []
        with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
            for record in reader:
                campaign_id = to_string(record.campaign_id)
                if not campaign_id:
                    continue
                valid_campaigns.append({
                    "campaign_id": campaign_id,
                    "camp_start_date": to_string(record.start_date),
                    "camp_end_date": to_string(record.end_date)
                })

        print(f"[{get_etl_time()}] ✅ 有效活动数：{len(valid_campaigns)}")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"查询活动列表失败：{str(e)}")


# ===================== 拉取报表数据 =====================
def fetch_task(task, token, dt):
    global total_collected
    camp, reg, aud, plt, pos = task
    cid = camp["campaign_id"]
    # 活动起止日期
    camp_start_date = camp["camp_start_date"]
    camp_end_date = camp["camp_end_date"]

    full_url = ""
    start_time = time.time()

    # 活动时间有效性判断
    try:
        s = datetime.strptime(camp_start_date, "%Y-%m-%d")
        e = datetime.strptime(camp_end_date, "%Y-%m-%d")
        c = datetime.strptime(dt, "%Y-%m-%d")
        if not (s <= c <= e):
            return
    except:
        return

    # 构建请求参数
    params = {
        "access_token": token,
        "campaign_id": cid,
        "date": dt,
        "metrics": REPORT_PARAMS["metrics"],
        "by_region": reg,
        "by_audience": aud,
        "platform": plt,
        "by_position": pos
    }

    try:
        req = Request('GET', CONFIG["api"]["report_url"], params=params)
        prepared = req.prepare()
        full_url = prepared.url

        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])

        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
            if not metric:
                continue

            pre_parse_raw_text = json.dumps(item, ensure_ascii=False)
            spot_id_val = safe_str(attr.get("spot_id"))
            spot_id_str_val = safe_str(attr.get("spot_id_str"))

            # ===================== 【核心】严格匹配最新建表语句的字段顺序 =====================
            row = [
                # 1. 请求参数
                safe_str(cid),  # request_campaign_id
                safe_str(dt),  # request_date
                safe_str(REPORT_PARAMS["metrics"]),  # request_metrics
                safe_str(reg),  # request_by_region
                safe_str(aud),  # request_by_audience
                safe_str(plt),  # request_platform
                safe_str(pos),  # request_by_position

                # 2. 活动基础信息
                safe_str(cid),  # campaign_id
                safe_str(camp_start_date),  # camp_start_date
                safe_str(camp_end_date),  # camp_end_date

                # 3. 接口返回基础字段
                safe_str(data.get("date")),  # result_date
                safe_str(data.get("version")),  # s_version
                safe_str(attr.get("publisher_id")),  # publisher_id
                spot_id_val,  # spot_id
                spot_id_str_val,  # spot_id_str
                safe_str(attr.get("audience")),  # audience
                safe_str(attr.get("universe")),  # universe
                safe_str(attr.get("region_id")),  # region_id
                safe_str(attr.get("target_id")),  # target_id
                safe_str(attr.get("keyword_id")),  # keyword_id
                safe_str(data.get("platform")),  # platform
                safe_str(data.get("total_spot_num")),  # total_spot_num

                # 4. 核心指标
                safe_str(metric.get("imp_acc")),  # imp_acc
                safe_str(metric.get("clk_acc")),  # clk_acc
                safe_str(metric.get("uim_acc")),  # uim_acc
                safe_str(metric.get("ucl_acc")),  # ucl_acc
                safe_str(metric.get("imp_day")),  # imp_day
                safe_str(metric.get("clk_day")),  # clk_day
                safe_str(metric.get("uim_day")),  # uim_day
                safe_str(metric.get("ucl_day")),  # ucl_day
                safe_str(metric.get("imp_avg_day")),  # imp_avg_day
                safe_str(metric.get("clk_avg_day")),  # clk_avg_day
                safe_str(metric.get("uim_avg_day")),  # uim_avg_day
                safe_str(metric.get("ucl_avg_day")),  # ucl_avg_day

                # 5. 曝光小时维度 0-23
                safe_str(metric.get("imp_h0")), safe_str(metric.get("imp_h1")),
                safe_str(metric.get("imp_h2")), safe_str(metric.get("imp_h3")),
                safe_str(metric.get("imp_h4")), safe_str(metric.get("imp_h5")),
                safe_str(metric.get("imp_h6")), safe_str(metric.get("imp_h7")),
                safe_str(metric.get("imp_h8")), safe_str(metric.get("imp_h9")),
                safe_str(metric.get("imp_h10")), safe_str(metric.get("imp_h11")),
                safe_str(metric.get("imp_h12")), safe_str(metric.get("imp_h13")),
                safe_str(metric.get("imp_h14")), safe_str(metric.get("imp_h15")),
                safe_str(metric.get("imp_h16")), safe_str(metric.get("imp_h17")),
                safe_str(metric.get("imp_h18")), safe_str(metric.get("imp_h19")),
                safe_str(metric.get("imp_h20")), safe_str(metric.get("imp_h21")),
                safe_str(metric.get("imp_h22")), safe_str(metric.get("imp_h23")),

                # 6. 点击小时维度 0-23
                safe_str(metric.get("clk_h0")), safe_str(metric.get("clk_h1")),
                safe_str(metric.get("clk_h2")), safe_str(metric.get("clk_h3")),
                safe_str(metric.get("clk_h4")), safe_str(metric.get("clk_h5")),
                safe_str(metric.get("clk_h6")), safe_str(metric.get("clk_h7")),
                safe_str(metric.get("clk_h8")), safe_str(metric.get("clk_h9")),
                safe_str(metric.get("clk_h10")), safe_str(metric.get("clk_h11")),
                safe_str(metric.get("clk_h12")), safe_str(metric.get("clk_h13")),
                safe_str(metric.get("clk_h14")), safe_str(metric.get("clk_h15")),
                safe_str(metric.get("clk_h16")), safe_str(metric.get("clk_h17")),
                safe_str(metric.get("clk_h18")), safe_str(metric.get("clk_h19")),
                safe_str(metric.get("clk_h20")), safe_str(metric.get("clk_h21")),
                safe_str(metric.get("clk_h22")), safe_str(metric.get("clk_h23")),

                # 7. 日志字段
                safe_str(full_url),  # full_request_url
                pre_parse_raw_text,  # pre_parse_raw_text
                get_log()  # etl_datetime
            ]

            all_data.append(row)
            total_collected += 1

    except Exception as e:
        print(f"[{get_log()}] ❌ 活动 {cid} 请求失败：{str(e)[:120]}")
    finally:
        elapsed_time = round(time.time() - start_time, 2)
        print(f"[{get_log()}] 🔗 活动 {cid} | 耗时={elapsed_time}s | URL: {full_url[:150]}")


# ===================== 主流程 =====================
def main():
    run_date = format_date(DT)
    print(f"[{get_log()}] 🚀 开始采集 | 分区 dt={DT}")

    username, password = get_adm_api_credentials()
    CONFIG["api"]["auth"]["username"] = username
    CONFIG["api"]["auth"]["password"] = password

    token = get_token()
    campaigns = get_campaign_list()

    if not campaigns:
        print("❌ 无有效活动")
        return

    # 构建任务
    tasks = []
    for c in campaigns:
        for r in REPORT_PARAMS["by_region"]:
            for a in REPORT_PARAMS["by_audience"]:
                for p in REPORT_PARAMS["platform"]:
                    for pos in REPORT_PARAMS["by_position"]:
                        tasks.append((c, r, a, p, pos))

    # 多线程采集
    with ThreadPoolExecutor(CONFIG["api"]["api_workers"]) as pool:
        futures = [pool.submit(fetch_task, t, token, run_date) for t in tasks]
        wait(futures)

    # 写入ODPS
    write_to_odps_partition(CONFIG["odps_table"], all_data)

    print("\n" + "=" * 60)
    print(f"🎉 任务完成")
    print(f"采集条数：{total_collected}")
    print(f"项目：{ODPS_PROJECT}")
    print(f"表名：{CONFIG['odps_table']}")
    print(f"分区：dt={DT}")
    print("=" * 60)


if __name__ == "__main__":
    main()
