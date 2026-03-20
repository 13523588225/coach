# -*- coding: utf-8 -*-
import requests
import json
import time
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from odps import ODPS, errors
from concurrent.futures import ThreadPoolExecutor, as_completed  # 新增：并行执行依赖

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {"username": "Coach_api", "password": "Coachapi2026"},
    "timeout": 30,
    "request_interval": 0.01  # 修改：休眠时间改为0.01秒
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project  # 自动获取当前DataWorks项目名
TARGET_TABLE = "coach_marketing_hub_dev.ods_mz_tvm_basic_show_api_di"  # 指定目标表

# 3. 核心日期配置（按此范围生成每日分区）
START_DT = '20260101'  # 起始日期（8位，yyyyMMdd）
END_DT = '20260316'  # 结束日期（8位，yyyyMMdd）

# 4. 接口固定参数（必传+遍历）
REPORT_PARAMS = {
    "metrics": "all",  # 必传：获取所有指标
    "by_position": "spot",  # 必传：按广告位维度
    "by_region_list": ["level0", "level1", "level2"]  # 遍历地区维度
}

# 5. 新增：并行/批次配置
PARALLEL_CONFIG = {
    "max_workers": 5,  # 并行度设为5
    "batch_size": 20000  # 每批次写入20000条
}


# ===================== 核心工具函数 =====================
def get_etl_time() -> str:
    """获取当前ETL时间戳（yyyy-MM-dd HH:mm:ss），对应etl_datetime字段"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_convert(date_str: str, to_format: str) -> str:
    """日期格式转换：yyyyMMdd <-> yyyy-MM-dd"""
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def get_date_range_by_start_end(start_dt: str, end_dt: str) -> List[str]:
    """按START_DT和END_DT生成每日日期列表（yyyyMMdd），每个日期对应一个分区"""
    dates = []
    try:
        current_dt = datetime.strptime(start_dt, "%Y%m%d")
        end_date_obj = datetime.strptime(end_dt, "%Y%m%d")
        while current_dt <= end_date_obj:
            dates.append(current_dt.strftime("%Y%m%d"))
            current_dt += timedelta(days=1)
    except ValueError as e:
        raise Exception(f"日期范围生成失败：{str(e)}")
    return dates


def is_date_in_campaign_valid(check_date: str, camp_start: str, camp_end: str) -> bool:
    """校验日期是否在活动有效期内（check_date:8位；camp_start/camp_end:10位）"""
    if not all([check_date, camp_start, camp_end]):
        print(f"⚠️ 校验失败：日期/活动有效期为空（检查日期：{check_date}）")
        return False
    try:
        check_dt_obj = datetime.strptime(check_date, "%Y%m%d")
        camp_start_obj = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_end_obj = datetime.strptime(camp_end, "%Y-%m-%d")
        is_valid = camp_start_obj <= check_dt_obj <= camp_end_obj
        if not is_valid:
            print(f"⏩ 日期{check_date}不在活动有效期[{camp_start}~{camp_end}]内，跳过")
        return is_valid
    except ValueError as e:
        print(f"⚠️ 日期格式错误（检查日期：{check_date}）：{str(e)}")
        return False


def to_string(value) -> str:
    """统一空值/None处理，强制转换为字符串"""
    if value is None or value == "" or value == "null":
        return ""
    return str(value)


# ===================== 秒针接口调用（适配并行）=====================
def get_miaozhen_token() -> str:
    """获取秒针接口鉴权Token"""
    try:
        resp = requests.post(
            API_CONFIG["token_url"],
            data=API_CONFIG["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()
        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误：{token_data.get('error_message')}")
        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token返回为空")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表（含活动开始/结束日期，匹配表start_date/end_date字段）"""
    try:
        resp = requests.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])
        valid_campaigns = []
        for c in campaigns:
            if not isinstance(c, dict) or not c.get("campaign_id"):
                continue
            valid_campaigns.append({
                "campaign_id": to_string(c.get("campaign_id")),
                "camp_start_date": to_string(c.get("start_time")),  # 活动开始日期（10位）
                "camp_end_date": to_string(c.get("end_time")),  # 活动结束日期（10位）
                "camp_start_dt": to_string(c.get("start_time")),  # 兼容字段
                "camp_end_dt": to_string(c.get("end_time"))  # 兼容字段
            })
        print(f"✅ 采集到有效活动列表：共{len(valid_campaigns)}个")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")


def parse_single_campaign(token: str, campaign: Dict, daily_dt: str) -> List[List]:
    """
    并行执行函数：解析单个活动的单日期数据
    :param token: 秒针Token
    :param campaign: 单个活动信息
    :param daily_dt: 分区日期（8位）
    :return: 该活动的待写入数据列表
    """
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]

    # 校验日期是否在活动有效期内
    if not is_date_in_campaign_valid(daily_dt, camp_start, camp_end):
        return campaign_data

    # 转换日期格式
    report_date_10bit = date_convert(daily_dt, "10位")
    if not report_date_10bit:
        print(f"⚠️ 日期{daily_dt}格式转换失败，跳过活动{camp_id}")
        return campaign_data

    # 遍历by_region维度
    for by_region in REPORT_PARAMS["by_region_list"]:
        try:
            # 构造请求参数
            request_params = {
                "campaign_id": camp_id,
                "date": report_date_10bit,
                "metrics": REPORT_PARAMS["metrics"],
                "by_position": REPORT_PARAMS["by_position"],
                "by_region": by_region
            }
            # 调用接口
            resp = requests.get(
                f"{API_CONFIG['report_basic_url']}?access_token={token}",
                params=request_params,
                timeout=API_CONFIG["timeout"],
                verify=False
            )
            resp.raise_for_status()
            raw_data = resp.json()

            # 校验接口返回
            if raw_data.get("error_code") != 0:
                print(f"⚠️ 活动{camp_id} by_region={by_region} 返回错误：{raw_data.get('error_message')}")
                time.sleep(API_CONFIG["request_interval"])
                continue
            result = raw_data.get("result", {})
            if not all([result.get("date"), result.get("campaignId"), result.get("items")]):
                print(f"⚠️ 活动{camp_id} by_region={by_region} 核心字段缺失")
                time.sleep(API_CONFIG["request_interval"])
                continue

            # 解析items
            items = result.get("items", [])
            etl_datetime = get_etl_time()
            for item in items:
                if not isinstance(item, dict):
                    continue
                attributes = item.get("attributes", {})
                metrics = item.get("metrics", {}) if item.get("metrics") is not None else {}

                # 组装写入行（匹配建表字段）
                write_row = [
                    to_string(result.get("campaignId")),
                    to_string(camp_start),
                    to_string(camp_end),
                    to_string(result.get("date")),
                    to_string(result.get("version")),
                    "",  # platform
                    "",  # total_spot_num
                    to_string(attributes.get("audience")),
                    "",  # target_id
                    "",  # publisher_id
                    "",  # spot_id
                    "",  # keyword_id
                    to_string(attributes.get("region_id")),
                    to_string(attributes.get("universe")),
                    to_string(metrics.get("imp_acc")),
                    to_string(metrics.get("clk_acc")),
                    to_string(metrics.get("uim_acc")),
                    to_string(metrics.get("ucl_acc")),
                    to_string(metrics.get("imp_day")),
                    to_string(metrics.get("clk_day")),
                    to_string(metrics.get("uim_day")),
                    to_string(metrics.get("ucl_day")),
                    to_string(metrics.get("imp_avg_day")),
                    to_string(metrics.get("clk_avg_day")),
                    to_string(metrics.get("uim_avg_day")),
                    to_string(metrics.get("ucl_avg_day")),
                    "",  # imp_acc_h00
                    "",  # imp_acc_h23
                    "",  # clk_acc_h00
                    "",  # clk_acc_h23
                    to_string(json.dumps(item, ensure_ascii=False)),
                    to_string(etl_datetime)
                ]
                campaign_data.append(write_row)

            time.sleep(API_CONFIG["request_interval"])
        except Exception as e:
            print(f"⚠️ 解析活动{camp_id} by_region={by_region} 失败：{str(e)}")
            time.sleep(API_CONFIG["request_interval"])
            continue

    return campaign_data


# ===================== ODPS写入核心函数（修改批次大小）=====================
def write_to_odps_partition(table_name: str, data: List[List], dt_partition: str):
    """
    按每日分区写入ODPS（清空分区+分批写入，批次大小改为20000）
    """
    if not data:
        print(f"⚠️ {table_name} 分区{dt_partition}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt_partition}'"

    try:
        # 清空已有分区
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 已清空分区：{table_name}.{partition_spec}")

        # 分批写入（批次大小改为20000）
        batch_size = PARALLEL_CONFIG["batch_size"]
        total_count = len(data)
        batch_num = (total_count + batch_size - 1) // batch_size  # 向上取整计算批次

        for i in range(batch_num):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_count)
            batch_data = data[start_idx:end_idx]

            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)

            print(f"🔄 分区{dt_partition} - 写入批次 {i + 1}/{batch_num}：{len(batch_data)}条")

        print(f"✅ 分区写入完成：{table_name} | 分区{dt_partition} | 总条数{total_count}")
    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")
    except Exception as e:
        raise Exception(f"分区写入异常：{str(e)}")


# ===================== 主流程（整合并行逻辑）=====================
def main():
    """
    核心执行流程：
    1. 按START_DT/END_DT生成每日分区
    2. 多线程并行解析不同活动的数据（并行度5）
    3. 合并数据后按20000条/批写入ODPS
    """
    try:
        # 任务初始化日志
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"分区范围：{START_DT} ~ {END_DT} | 目标表：{TARGET_TABLE}")
        print(f"并行配置：max_workers={PARALLEL_CONFIG['max_workers']} | batch_size={PARALLEL_CONFIG['batch_size']}")
        print(
            f"接口参数：metrics={REPORT_PARAMS['metrics']} | by_position={REPORT_PARAMS['by_position']} | request_interval={API_CONFIG['request_interval']}")

        # 1. 获取秒针Token
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 2. 获取活动列表
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 3. 生成每日分区日期列表
        daily_partition_dates = get_date_range_by_start_end(START_DT, END_DT)
        print(f"✅ 生成每日分区：共{len(daily_partition_dates)}天")

        # 4. 按每日分区处理数据（并行解析）
        for daily_dt in daily_partition_dates:
            print(f"\n========== 处理分区日期：{daily_dt} ==========")
            daily_write_data = []

            # 初始化线程池（并行度5）
            with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["max_workers"]) as executor:
                # 提交并行任务
                future_to_campaign = {
                    executor.submit(parse_single_campaign, token, campaign, daily_dt): campaign
                    for campaign in campaign_list
                }

                # 收集并行结果
                for future in as_completed(future_to_campaign):
                    campaign = future_to_campaign[future]
                    try:
                        campaign_data = future.result()
                        if campaign_data:
                            daily_write_data.extend(campaign_data)
                    except Exception as e:
                        print(f"❌ 并行解析活动{campaign['campaign_id']}失败：{str(e)}")
                        continue

            # 5. 写入ODPS分区（20000条/批）
            if daily_write_data:
                write_to_odps_partition(TARGET_TABLE, daily_write_data, daily_dt)
            else:
                print(f"⚠️ 分区{daily_dt}无有效数据，跳过写入")

        # 任务结束
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📊 任务总结：")
        print(f"   - 处理分区数：{len(daily_partition_dates)}个")
        print(f"   - 并行度：{PARALLEL_CONFIG['max_workers']} | 批次大小：{PARALLEL_CONFIG['batch_size']}条")

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()