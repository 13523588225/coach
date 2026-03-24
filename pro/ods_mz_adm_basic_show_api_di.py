# -*- coding: utf-8 -*-
"""
秒针广告API数据采集 - 生产稳定版（带完整注释）
功能：API并行10 + 写入MaxCompute并行5 + 每批次20000条
"""
import json
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from queue import Queue
import requests
import urllib3
from odps import ODPS, options

# 关闭SSL不安全请求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 获取当前ODPS项目
ODPS_PROJECT = ODPS().project

# ===================== 核心配置 =====================
CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,  # MaxCompute项目名
        "table_name": "ods_mz_adm_basic_show_api_di",  # 目标表名
        "batch_size": 20000,  # 每20000条写入一次
        "dt": "20260301",  # 分区日期
        "write_workers": 5  # 写入并行线程数
    },
    "report_params": {
        # 报表维度组合
        "by_region_list": ["level0", "level1", "level2"],
        "by_audience_list": ["overall", "stable", "target"],
        "platform_list": ["pc", "pm", "mb"],
        "by_position_list": ["campaign", "publisher", "spot", "keyword"]
    },
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",  # 获取token接口
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",  # 活动列表接口
        "report_url": "https://api.cn.miaozhen.com/admonitor/v1/reports/basic/show",  # 报表数据接口
        "auth": {  # 认证信息
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 60,  # 接口超时时间
        "api_workers": 10  # API请求并行线程数
    }
}

# ===================== 全局变量 =====================
write_queue = Queue(maxsize=100000)  # 数据缓冲队列（防OOM）
write_finished = False  # 写入完成标记
total_collected = 0  # 总采集条数
total_written = 0  # 总写入条数


# ===================== 工具方法 =====================

def get_log_time():
    """获取日志时间戳（精确到毫秒）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def safe_str(val):
    """安全字符串转换：空值/异常值统一转为空字符串"""
    if val is None or val == "" or val == "-" or str(val).lower() in ("null", "undefined"):
        return ""
    return str(val).replace("\n", " ").replace("\r", "")


def format_date(dt_str):
    """日期格式化：把 20260301 转为 2026-03-01"""
    return datetime.strptime(dt_str, "%Y%m%d").strftime("%Y-%m-%d")


def is_date_in_range(start_date, end_date, check_date):
    """
    判断查询日期是否在活动时间范围内
    :param start_date: 活动开始日期 YYYY-MM-DD
    :param end_date: 活动结束日期 YYYY-MM-DD
    :param check_date: 要查询的日期 YYYY-MM-DD
    :return: 在范围内返回True，否则False
    """
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
        c = datetime.strptime(check_date, "%Y-%m-%d")
        return s <= c <= e
    except:
        return False


# ===================== MaxCompute 写入相关 =====================

def init_odps_writer(partition_dt):
    """
    初始化MaxCompute写入器
    1. 获取表对象
    2. 删除旧分区
    3. 创建新分区并返回写入句柄
    """
    odps = ODPS(project=CONFIG["odps"]["project"])
    options.tunnel.use_instance_tunnel = True  # 开启实例Tunnel加速写入
    table = odps.get_table(CONFIG["odps"]["table_name"])

    # 删除旧分区避免重复数据
    partition_spec = f"dt='{partition_dt}'"
    try:
        odps.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION IF EXISTS ({partition_spec})")
    except:
        pass

    # 打开分区写入器
    return table.open_writer(partition=partition_spec, create_partition=True)


def write_worker(worker_id, odps_writer, dt):
    """
    写入线程：从队列取数据批量写入MaxCompute
    每凑够 batch_size 条写入一次
    """
    global total_written
    batch_size = CONFIG["odps"]["batch_size"]

    while not (write_finished and write_queue.empty()):
        batch = []
        # 凑够一批数据
        while len(batch) < batch_size and not write_queue.empty():
            batch.append(write_queue.get())

        if batch:
            start = time.time()
            odps_writer.write(batch)  # 批量写入
            cost = round(time.time() - start, 3)
            total_written += len(batch)
            print(f"[{get_log_time()}] ✅ 写入线程{worker_id} | 批次{len(batch)}条 | 耗时{cost}s | 累计{total_written}")


# ===================== API 采集相关 =====================

def get_access_token():
    """获取API访问凭证token"""
    start = time.time()
    resp = requests.post(
        CONFIG["api"]["token_url"],
        data=CONFIG["api"]["auth"],
        timeout=60,
        verify=False
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    cost = round(time.time() - start, 3)
    print(f"[{get_log_time()}] 🔑 获取Token成功 | 耗时{cost}s")
    return token


def get_campaign_list(token):
    """
    获取所有活动列表
    只保留 campaign_id、start_date、end_date 都存在的活动
    """
    start = time.time()
    url = f"{CONFIG['api']['campaign_url']}?access_token={token}"
    resp = requests.get(url, timeout=60, verify=False)
    resp.raise_for_status()
    campaigns = []
    for item in resp.json():
        cid = item.get("campaign_id")
        sdt = item.get("start_date")
        edt = item.get("end_date")
        if cid and sdt and edt:
            campaigns.append({
                "campaign_id": str(cid),
                "start_date": sdt,
                "end_date": edt
            })
    cost = round(time.time() - start, 3)
    print(f"[{get_log_time()}] 📋 有效活动数 {len(campaigns)} | 耗时{cost}s")
    return campaigns


def fetch_report_data(task, token, check_date):
    """
    拉取单个活动+维度组合的报表数据
    1. 校验活动时间
    2. 发起API请求
    3. 解析数据并放入写入队列
    """
    global total_collected
    camp, region, audience, platform, position = task
    camp_id = camp["campaign_id"]
    start_date = camp["start_date"]
    end_date = camp["end_date"]

    # 不在活动时间范围内直接跳过
    if not is_date_in_range(start_date, end_date, check_date):
        return

    # 构造API请求参数
    params = {
        "access_token": token,
        "campaign_id": camp_id,
        "date": check_date,
        "metrics": "all",
        "by_region": region,
        "by_audience": audience,
        "platform": platform,
        "by_position": position
    }

    try:
        # 请求API并计时
        start = time.time()
        resp = requests.get(CONFIG["api"]["report_url"], params=params, timeout=60, verify=False)
        resp.raise_for_status()
        cost = round(time.time() - start, 3)
        data = resp.json()
        items = data.get("items", [])

        # 解析每一行数据
        rows = []
        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})
            row_raw = json.dumps(item, ensure_ascii=False)
            req_params = json.dumps(params, ensure_ascii=False)
            etl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 组装表字段（严格与表结构一致）
            row = [
                safe_str(camp_id), safe_str(start_date), safe_str(end_date), safe_str(data.get("date")),
                safe_str(position), safe_str(region), "all", safe_str(audience), safe_str(platform),
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
                req_params, row_raw, etl_time
            ]
            rows.append(row)

        # 写入队列
        for r in rows:
            write_queue.put(r)

        total_collected += len(rows)
        print(
            f"[{get_log_time()}] 📥 {camp_id} | {position}-{region}-{audience}-{platform} | {len(rows)}条 | API耗时{cost}s")

    except Exception as e:
        # 失败不中断程序
        print(f"[{get_log_time()}] ❌ {camp_id} 采集失败")


# ===================== 主流程 =====================

def main():
    global write_finished
    dt = CONFIG["odps"]["dt"]
    check_date = format_date(dt)

    print(f"[{get_log_time()}] 🚀 开始采集 | 分区dt={dt} | API并行10 | 写入并行5 | 批次20000")

    # 1. 获取token
    token = get_access_token()

    # 2. 获取活动列表
    campaign_list = get_campaign_list(token)
    if not campaign_list:
        print(f"[{get_log_time()}] ❌ 无有效活动，程序退出")
        return

    # 3. 生成所有维度组合任务
    tasks = []
    for camp in campaign_list:
        for r in CONFIG["report_params"]["by_region_list"]:
            for a in CONFIG["report_params"]["by_audience_list"]:
                for p in CONFIG["report_params"]["platform_list"]:
                    for pos in CONFIG["report_params"]["by_position_list"]:
                        tasks.append((camp, r, a, p, pos))

    print(f"[{get_log_time()}] 🧩 总任务数：{len(tasks)}")

    # 4. 初始化5个MaxCompute写入器
    writers = [init_odps_writer(dt) for _ in range(CONFIG["odps"]["write_workers"])]

    # 5. 启动写入线程池
    with ThreadPoolExecutor(max_workers=CONFIG["odps"]["write_workers"]) as write_pool:
        for i, w in enumerate(writers):
            write_pool.submit(write_worker, i, w, dt)

        # 6. 启动API并行采集
        with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as api_pool:
            futures = [api_pool.submit(fetch_report_data, t, token, check_date) for t in tasks]
            wait(futures)

    # 7. 等待写入完成
    write_finished = True
    time.sleep(5)

    # 8. 关闭写入器
    for w in writers:
        w.close()

    # 9. 最终统计
    print("\n" + "=" * 60)
    print(f"[{get_log_time()}] 🎉 任务全部完成！")
    print(f"分区：dt={dt}")
    print(f"总采集：{total_collected} 条")
    print(f"总写入：{total_written} 条")
    print("=" * 60)


if __name__ == "__main__":
    main()