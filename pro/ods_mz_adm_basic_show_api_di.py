# -*- coding: utf-8 -*-
"""
秒针广告API采集 - 最终版（字段严格匹配用户表结构）
API并行：10
写入：一次性极速写入MaxCompute
功能：metrics为空 → 自动跳过不入库
"""
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
import requests
import urllib3
from odps import ODPS, options

# 禁用HTTPS警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== 全局配置（按需修改） =====================
# ODPS客户端初始化（需确保环境已配置AK/SK，或手动传入：ODPS(access_id='xxx', secret_access_key='xxx', project='xxx', endpoint='xxx')）
odps_client = ODPS()
ODPS_PROJECT = odps_client.project

CONFIG = {
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "ods_mz_adm_basic_show_api_di",  # 目标MaxCompute表名
        "dt": "20260301"  # 采集日期（分区字段）
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
        "api_workers": 10  # API并行数
    }
}

# 全局变量：存储所有有效数据、统计采集总数
all_data = []
total_collected = 0

# ===================== 工具方法 =====================
def get_log():
    """获取格式化日志时间"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_str(val):
    """安全字符串处理：空值/特殊值转空字符串，去除换行符"""
    if val is None or val == "" or val == "-" or str(val).lower() in ["null", "undefined"]:
        return ""
    return str(val).replace("\n", " ").replace("\r", "")

def format_date(dt):
    """日期格式转换：YYYYMMDD → YYYY-MM-DD"""
    return datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")

def is_in_range(start, end, check):
    """检查日期是否在活动有效期内"""
    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        c = datetime.strptime(check, "%Y-%m-%d")
        return s <= c <= e
    except Exception as e:
        print(f"[{get_log()}] ⚠️ 日期校验失败：{e}")
        return False

# ===================== API 认证 =====================
def get_token():
    """获取API访问TOKEN"""
    start_time = time.time()
    try:
        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=CONFIG["api"]["auth"],
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()  # 非200响应抛出异常
        token = resp.json()["access_token"]
        print(f"[{get_log()}] 🔑 获取TOKEN成功，耗时 {round(time.time() - start_time, 2)}s")
        return token
    except Exception as e:
        print(f"[{get_log()}] ❌ 获取TOKEN失败：{e}")
        raise

# ===================== 获取活动列表 =====================
def get_campaigns(token):
    """获取有效活动列表（过滤有ID/起止日期的活动）"""
    start_time = time.time()
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        camps = []
        for item in resp.json():
            cid = item.get("campaign_id")
            sdt = item.get("start_date")
            edt = item.get("end_date")
            if cid and sdt and edt:
                camps.append({
                    "campaign_id": str(cid),
                    "start_date": sdt,
                    "end_date": edt
                })
        print(f"[{get_log()}] 📋 筛选出有效活动 {len(camps)} 个，耗时 {round(time.time() - start_time, 2)}s")
        return camps
    except Exception as e:
        print(f"[{get_log()}] ❌ 获取活动列表失败：{e}")
        raise

# ===================== 拉取单任务报表数据 =====================
def fetch_task(task, token, dt):
    """
    单任务数据采集：按活动/维度组合拉取API数据
    :param task: 任务元组 (camp, reg, aud, plt, pos)
    :param token: API访问TOKEN
    :param dt: 采集日期（YYYY-MM-DD）
    """
    global total_collected
    camp, reg, aud, plt, pos = task
    cid = camp["campaign_id"]

    # 跳过不在活动有效期内的数据
    if not is_in_range(camp["start_date"], camp["end_date"], dt):
        print(f"[{get_log()}] ⚠️ 活动 {cid} 不在 {dt} 有效期内，跳过")
        return

    # 构造API请求参数
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
        resp = requests.get(
            CONFIG["api"]["report_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        raw_response = resp.text

        # 解析数据行（严格匹配MaxCompute表字段顺序）
        rows = []
        for item in items:
            attr = item.get("attributes", {})
            metric = item.get("metrics", {})

            # metrics为空直接跳过（核心逻辑）
            if not metric or len(metric) == 0:
                continue

            # 字段顺序 100% 匹配CREATE TABLE结构
            row = [
                safe_str(cid),                      # campaign_id
                safe_str(camp["start_date"]),       # campaign_start_date
                safe_str(camp["end_date"]),         # campaign_end_date
                safe_str(data.get("date")),         # report_day_date
                safe_str(pos),                      # by_position
                safe_str(reg),                      # by_region
                "all",                              # metrics
                safe_str(aud),                      # by_audience
                safe_str(plt),                      # platform
                safe_str(data.get("version")),      # s_version
                safe_str(data.get("platform")),     # platform_resp
                safe_str(data.get("total_spot_num")),# total_spot_num
                safe_str(attr.get("audience")),     # audience
                safe_str(attr.get("target_id")),    # target_id
                safe_str(attr.get("publisher_id")), # publisher_id
                safe_str(attr.get("spot_id")),      # spot_id
                safe_str(attr.get("keyword_id")),   # keyword_id
                safe_str(attr.get("region_id")),    # region_id
                safe_str(attr.get("universe")),     # universe
                safe_str(metric.get("imp_acc")),    # imp_acc
                safe_str(metric.get("clk_acc")),    # clk_acc
                safe_str(metric.get("uim_acc")),    # uim_acc
                safe_str(metric.get("ucl_acc")),    # ucl_acc
                safe_str(metric.get("imp_day")),    # imp_day
                safe_str(metric.get("clk_day")),    # clk_day
                safe_str(metric.get("uim_day")),    # uim_day
                safe_str(metric.get("ucl_day")),    # ucl_day
                safe_str(metric.get("imp_avg_day")),# imp_avg_day
                safe_str(metric.get("clk_avg_day")),# clk_avg_day
                safe_str(metric.get("uim_avg_day")),# uim_avg_day
                safe_str(metric.get("ucl_avg_day")),# ucl_avg_day
                safe_str(metric.get("imp_h00")),    # imp_acc_h00
                safe_str(metric.get("imp_h23")),    # imp_acc_h23
                safe_str(metric.get("clk_h00")),    # clk_acc_h00
                safe_str(metric.get("clk_h23")),    # clk_acc_h23
                json.dumps(params, ensure_ascii=False),  # request_params
                raw_response,                       # pre_parse_raw_text
                get_log()                           # etl_datetime
            ]
            rows.append(row)

        # 汇总数据到全局列表
        all_data.extend(rows)
        total_collected += len(rows)
        print(f"[{get_log()}] 📥 活动 {cid} | 维度 {reg}-{aud}-{plt}-{pos} | 有效数据 {len(rows)} 条 | 累计 {total_collected} 条")

    except Exception as e:
        print(f"[{get_log()}] ❌ 采集任务失败（活动 {cid} | 维度 {reg}-{aud}-{plt}-{pos}）：{e}")

# ===================== 极速写入 MaxCompute =====================
def write_all_to_odps(dt, data_list):
    """
    批量写入MaxCompute（先删分区再写入，保证数据幂等）
    :param dt: 分区日期（YYYYMMDD）
    :param data_list: 待写入数据列表
    """
    if not data_list:
        print(f"[{get_log()}] ❌ 无有效数据可写入MaxCompute")
        return

    # 初始化ODPS表对象
    table = odps_client.get_table(f"{CONFIG['odps']['project']}.{CONFIG['odps']['table_name']}")
    partition = f"dt='{dt}'"  # 分区格式：dt='20260301'

    # 先删除已有分区（避免数据重复）
    try:
        odps_client.execute_sql(f"ALTER TABLE {CONFIG['odps']['table_name']} DROP PARTITION IF EXISTS ({partition});")
        print(f"[{get_log()}] 🗑️  已删除分区 {partition} 原有数据")
    except Exception as e:
        print(f"[{get_log()}] ⚠️ 分区删除失败（可能是首次写入）：{e}")

    # 批量写入数据
    start_time = time.time()
    try:
        with table.open_writer(partition=partition, create_partition=True) as writer:
            writer.write(data_list)  # 一次性写入所有数据
        print(f"[{get_log()}] ✅ 写入MaxCompute完成！共 {len(data_list)} 条 | 耗时 {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"[{get_log()}] ❌ 写入MaxCompute失败：{e}")
        raise

# ===================== 主函数 =====================
def main():
    """主流程：认证 → 获取活动 → 并行采集 → 写入MaxCompute"""
    # 初始化采集日期
    dt = CONFIG["odps"]["dt"]
    check_dt = format_date(dt)
    print(f"[{get_log()}] 🚀 开始秒针广告API采集（日期：{dt} | API并行数：{CONFIG['api']['api_workers']}）")

    try:
        # 1. 获取API Token
        token = get_token()

        # 2. 获取有效活动列表
        camps = get_campaigns(token)
        if not camps:
            print(f"[{get_log()}] ❌ 无有效活动，任务终止")
            return

        # 3. 构造所有采集任务（笛卡尔积组合维度）
        tasks = []
        for camp in camps:
            for reg in CONFIG["report_params"]["by_region_list"]:
                for aud in CONFIG["report_params"]["by_audience_list"]:
                    for plt in CONFIG["report_params"]["platform_list"]:
                        for pos in CONFIG["report_params"]["by_position_list"]:
                            tasks.append((camp, reg, aud, plt, pos))
        print(f"[{get_log()}] 📝 共生成 {len(tasks)} 个采集任务")

        # 4. 并行执行采集任务
        with ThreadPoolExecutor(max_workers=CONFIG["api"]["api_workers"]) as pool:
            futures = [pool.submit(fetch_task, task, token, check_dt) for task in tasks]
            wait(futures)  # 等待所有任务完成

        # 5. 写入MaxCompute
        write_all_to_odps(dt, all_data)

        # 6. 输出最终统计
        print("\n" + "=" * 60)
        print(f"[{get_log()}] 🎉 采集任务全部完成")
        print(f"📊 统计：总任务数 {len(tasks)} | 有效数据 {total_collected} 条 | 写入MaxCompute {len(all_data)} 条")
        print("=" * 60)

    except Exception as e:
        print(f"[{get_log()}] ❌ 主流程执行失败：{e}")
        raise

if __name__ == "__main__":
    # 启动主流程
    main()