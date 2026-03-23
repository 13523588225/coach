# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
from odps import ODPS, errors, TableSchema, Column, Partition
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {"username": "Coach_api", "password": "Coachapi2026"},
    "timeout": 30,
    "request_interval": 0.01  # 请求间隔0.01秒
}

# 2. ODPS配置
ODPS_PROJECT = ODPS().project
TARGET_TABLE = "coach_marketing_hub_dev.ods_mz_tvm_basic_show_api_di"

# 3. 日期配置
START_DT = '20260301'
END_DT = '20260305'

# 4. 接口固定参数
REPORT_PARAMS = {
    "metrics": "all",
    "by_position": "spot",
    "by_region_list": ["level0", "level1", "level2"]
}

# 5. 并行/批次配置（优化内存）
PARALLEL_CONFIG = {
    "campaign_max_workers": 10,  # 活动解析并行数
    "partition_max_workers": 3,  # 分区写入并行数（根据ODPS资源调整）
    "batch_size": 20000  # 每批次写入2万条
}

# 6. 小时粒度字段列表（h00~h23）
HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]


# ===================== 工具函数 =====================
def get_etl_time() -> str:
    """获取ETL时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_convert(date_str: str, to_format: str) -> str:
    """日期格式转换"""
    try:
        if to_format == "8位":
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
        elif to_format == "10位":
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def get_date_range_by_start_end(start_dt: str, end_dt: str) -> List[str]:
    """生成每日日期列表"""
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
    """校验日期是否在活动有效期内"""
    if not all([check_date, camp_start, camp_end]):
        return False
    try:
        check_dt_obj = datetime.strptime(check_date, "%Y%m%d")
        camp_start_obj = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_end_obj = datetime.strptime(camp_end, "%Y-%m-%d")
        return camp_start_obj <= check_dt_obj <= camp_end_obj
    except ValueError:
        return False


def clean_numeric_value(value: Any) -> int | float | None:
    """清理数值型字段（空值返回None，异常值返回0）"""
    if value is None or value == "" or value == "null":
        return None
    try:
        if isinstance(value, str):
            # 处理字符串形式的数值
            return int(value) if value.isdigit() else float(value)
        return value  # 原生数值直接返回
    except (ValueError, TypeError):
        return 0


def clean_string_value(value: Any) -> str:
    """清理字符串字段（空值返回空字符串）"""
    if value is None or value == "" or value == "null":
        return ""
    return str(value).strip()


# ===================== ODPS表结构管理 =====================
def create_mz_tv_table_if_not_exists():
    """创建秒针TV基础报表表（适配原生数据类型）"""
    o = ODPS(project=ODPS_PROJECT)

    # 表已存在则跳过
    if o.exist_table(TARGET_TABLE):
        print(f"✅ 表{TARGET_TABLE}已存在，跳过创建")
        return

    # 定义表结构（按字段语义区分类型）
    columns = [
        # 1. 请求参数字段（字符串）
        Column(name="request_campaign_id", type="STRING", comment="请求参数-活动ID"),
        Column(name="request_date", type="STRING", comment="请求参数-报表日期（10位）"),
        Column(name="request_metrics", type="STRING", comment="请求参数-指标类型"),
        Column(name="request_by_position", type="STRING", comment="请求参数-维度（点位）"),
        Column(name="request_by_region", type="STRING", comment="请求参数-地区维度"),

        # 2. 活动基础字段（混合类型）
        Column(name="campaign_id", type="STRING", comment="活动ID"),
        Column(name="camp_start_date", type="STRING", comment="活动开始日期（10位）"),
        Column(name="camp_end_date", type="STRING", comment="活动结束日期（10位）"),
        Column(name="report_date", type="STRING", comment="报表日期（10位）"),
        Column(name="version", type="BIGINT", comment="版本号"),
        Column(name="publisher_id", type="STRING", comment="发布方ID"),
        Column(name="spot_id", type="STRING", comment="点位ID"),
        Column(name="spot_id_str", type="STRING", comment="点位ID字符串"),
        Column(name="audience", type="STRING", comment="受众群体"),
        Column(name="universe", type="STRING", comment="总体人群"),
        Column(name="region_id", type="STRING", comment="地区ID"),

        # 3. 核心指标（BIGINT）
        Column(name="imp_acc", type="BIGINT", comment="累计曝光量"),
        Column(name="clk_acc", type="BIGINT", comment="累计点击量"),
        Column(name="uim_acc", type="BIGINT", comment="累计独立曝光用户数"),
        Column(name="ucl_acc", type="BIGINT", comment="累计独立点击用户数"),
        Column(name="imp_day", type="BIGINT", comment="当日曝光量"),
        Column(name="clk_day", type="BIGINT", comment="当日点击量"),
        Column(name="uim_day", type="BIGINT", comment="当日独立曝光用户数"),
        Column(name="ucl_day", type="BIGINT", comment="当日独立点击用户数"),
        Column(name="imp_avg_day", type="BIGINT", comment="日均曝光量"),
        Column(name="clk_avg_day", type="BIGINT", comment="日均点击量"),
        Column(name="uim_avg_day", type="BIGINT", comment="日均独立曝光用户数"),
        Column(name="ucl_avg_day", type="BIGINT", comment="日均独立点击用户数"),

        # 4. 小时粒度曝光指标（BIGINT）
        *[Column(name=f"imp_{hour}", type="BIGINT", comment=f"{hour}时曝光量") for hour in HOUR_FIELDS],

        # 5. 小时粒度点击指标（BIGINT）
        *[Column(name=f"clk_{hour}", type="BIGINT", comment=f"{hour}时点击量") for hour in HOUR_FIELDS],

        # 6. 元数据字段（字符串）
        Column(name="pre_parse_raw_text", type="STRING", comment="原始解析文本（attributes+metrics）"),
        Column(name="etl_datetime", type="STRING", comment="ETL处理时间戳")
    ]

    # 定义分区字段
    partitions = [Partition(name="dt", type="STRING", comment="分区日期（8位）")]

    # 创建表（使用ODPS默认配置）
    schema = TableSchema(columns=columns, partitions=partitions)
    o.create_table(
        name=TARGET_TABLE,
        schema=schema,
        if_not_exists=True
    )
    print(f"✅ 表{TARGET_TABLE}创建成功，适配原生数据类型")


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取鉴权Token"""
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
        return clean_string_value(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表"""
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
            if isinstance(c, dict) and c.get("campaign_id"):
                valid_campaigns.append({
                    "campaign_id": clean_string_value(c.get("campaign_id")),
                    "camp_start_date": clean_string_value(c.get("start_time")),
                    "camp_end_date": clean_string_value(c.get("end_time"))
                })
        print(f"✅ 采集到有效活动列表：共{len(valid_campaigns)}个")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"采集活动列表失败：{str(e)}")


def parse_single_campaign(token: str, campaign: Dict, daily_dt: str) -> Tuple[List[List[Any]], str, int]:
    """解析单个活动的单日期数据（返回数据+活动ID+数据量）"""
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]

    # 校验日期有效性
    if not is_date_in_campaign_valid(daily_dt, camp_start, camp_end):
        return campaign_data, camp_id, 0

    # 转换日期格式
    report_date_10bit = date_convert(daily_dt, "10位")
    if not report_date_10bit:
        return campaign_data, camp_id, 0

    # 遍历地区维度
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

            # 校验返回结果
            if raw_data.get("error_code") != 0:
                time.sleep(API_CONFIG["request_interval"])
                continue
            result = raw_data.get("result", {})
            if not all([result.get("date"), result.get("campaignId"), result.get("items")]):
                time.sleep(API_CONFIG["request_interval"])
                continue

            # 解析items数据
            items = result.get("items", [])
            etl_datetime = get_etl_time()

            for item in items:
                if not isinstance(item, dict):
                    continue
                attributes = item.get("attributes", {})
                metrics = item.get("metrics", {}) if item.get("metrics") is not None else {}

                # 生成当前行的pre_parse_raw_text
                current_row_data = {
                    "attributes": attributes,
                    "metrics": metrics
                }
                pre_parse_raw_text = clean_string_value(
                    json.dumps(current_row_data, ensure_ascii=False, indent=None)
                )

                # 1. 组装请求参数字段（字符串）
                request_fields = [
                    clean_string_value(request_params["campaign_id"]),
                    clean_string_value(request_params["date"]),
                    clean_string_value(request_params["metrics"]),
                    clean_string_value(request_params["by_position"]),
                    clean_string_value(request_params["by_region"])
                ]

                # 2. 组装活动基础字段（混合类型）
                base_fields = [
                    clean_string_value(result.get("campaignId")),
                    clean_string_value(camp_start),
                    clean_string_value(camp_end),
                    clean_string_value(result.get("date")),
                    clean_numeric_value(result.get("version")),
                    clean_string_value(attributes.get("publisher_id")),
                    clean_string_value(attributes.get("spot_id")),
                    clean_string_value(attributes.get("spot_id_str")),
                    clean_string_value(attributes.get("audience")),
                    clean_string_value(attributes.get("universe")),
                    clean_string_value(attributes.get("region_id")),
                    clean_numeric_value(metrics.get("imp_acc")),
                    clean_numeric_value(metrics.get("clk_acc")),
                    clean_numeric_value(metrics.get("uim_acc")),
                    clean_numeric_value(metrics.get("ucl_acc")),
                    clean_numeric_value(metrics.get("imp_day")),
                    clean_numeric_value(metrics.get("clk_day")),
                    clean_numeric_value(metrics.get("uim_day")),
                    clean_numeric_value(metrics.get("ucl_day")),
                    clean_numeric_value(metrics.get("imp_avg_day")),
                    clean_numeric_value(metrics.get("clk_avg_day")),
                    clean_numeric_value(metrics.get("uim_avg_day")),
                    clean_numeric_value(metrics.get("ucl_avg_day")),
                ]

                # 3. 组装小时粒度曝光指标（数值型）
                imp_hour_fields = [clean_numeric_value(metrics.get(f"imp_{hour}")) for hour in HOUR_FIELDS]

                # 4. 组装小时粒度点击指标（数值型）
                clk_hour_fields = [clean_numeric_value(metrics.get(f"clk_{hour}")) for hour in HOUR_FIELDS]

                # 5. 组装元数据字段（字符串）
                meta_fields = [
                    pre_parse_raw_text,
                    clean_string_value(etl_datetime)
                ]

                # 合并所有字段（保留原生类型）
                write_row = request_fields + base_fields + imp_hour_fields + clk_hour_fields + meta_fields
                campaign_data.append(write_row)

            time.sleep(API_CONFIG["request_interval"])
        except Exception:
            time.sleep(API_CONFIG["request_interval"])
            continue

    return campaign_data, camp_id, len(campaign_data)


def process_single_partition(token: str, campaign_list: List[Dict], daily_dt: str) -> Tuple[str, List[List[Any]]]:
    """处理单个分区（含解析进度监控），返回分区日期和数据"""
    print(f"\n🚀 开始处理分区：{daily_dt} | 待解析活动数：{len(campaign_list)}个")
    partition_data = []
    total_campaigns = len(campaign_list)
    completed_campaigns = 0
    total_generated_rows = 0

    # 并行解析活动数据
    with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["campaign_max_workers"]) as executor:
        future_to_campaign = {
            executor.submit(parse_single_campaign, token, campaign, daily_dt): campaign
            for campaign in campaign_list
        }

        # 收集结果（实时打印进度）
        for future in as_completed(future_to_campaign):
            campaign = future_to_campaign[future]
            try:
                campaign_data, camp_id, row_count = future.result()
                completed_campaigns += 1
                if campaign_data:
                    partition_data.extend(campaign_data)
                    total_generated_rows += row_count
                    # 打印单活动解析结果
                    print(f"📊 分区{daily_dt} - 活动{camp_id}解析完成：生成{row_count}条数据")
                else:
                    print(f"📊 分区{daily_dt} - 活动{camp_id}解析完成：无有效数据")

                # 打印分区解析进度（每完成5个活动或最后1个活动时打印）
                progress = round((completed_campaigns / total_campaigns) * 100, 2)
                if completed_campaigns % 5 == 0 or completed_campaigns == total_campaigns:
                    print(
                        f"⏳ 分区{daily_dt} - 解析进度：{completed_campaigns}/{total_campaigns}个活动（{progress}%），累计生成{total_generated_rows}条数据")
            except Exception as e:
                completed_campaigns += 1
                print(f"❌ 分区{daily_dt} - 活动{campaign['campaign_id']}解析失败：{str(e)}")
                # 失败后仍打印进度
                progress = round((completed_campaigns / total_campaigns) * 100, 2)
                if completed_campaigns % 5 == 0 or completed_campaigns == total_campaigns:
                    print(
                        f"⏳ 分区{daily_dt} - 解析进度：{completed_campaigns}/{total_campaigns}个活动（{progress}%），累计生成{total_generated_rows}条数据")
                continue

    print(f"✅ 分区{daily_dt} - 解析完成，共生成{len(partition_data)}条数据（累计解析{completed_campaigns}个活动）")
    return (daily_dt, partition_data)


# ===================== ODPS写入 =====================
def write_to_odps_partition(table_name: str, data: List[List[Any]], dt_partition: str):
    """按分区写入ODPS（含写入进度监控）"""
    if not data:
        print(f"⚠️ 分区{dt_partition}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"ODPS表不存在：{table_name}")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt_partition}'"
    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size  # 向上取整计算总批次

    try:
        # 清空已有分区
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 分区{dt_partition} - 已清空历史数据")

        # 打印批次规划信息
        print(f"📊 分区{dt_partition} - 写入规划：总数据量{total_count}条，分{batch_num}批次（每批次{batch_size}条）")

        # 分批写入（实时打印写入进度）
        total_batch_time = 0  # 累计所有批次耗时
        completed_batches = 0
        total_written_rows = 0

        for i in range(batch_num):
            # 记录批次开始时间
            batch_start_time = time.time()

            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_count)
            batch_data = data[start_idx:end_idx]
            batch_actual_count = len(batch_data)

            # 写入当前批次（原生数据类型适配ODPS表）
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)

            # 计算批次耗时（保留2位小数）
            batch_cost_time = round(time.time() - batch_start_time, 2)
            total_batch_time += batch_cost_time
            completed_batches += 1
            total_written_rows += batch_actual_count

            # 打印当前批次进度+耗时
            write_progress = round((completed_batches / batch_num) * 100, 2)
            print(
                f"🔄 分区{dt_partition} - 写入进度：{completed_batches}/{batch_num}批次（{write_progress}%）| 本批写入{batch_actual_count}条 | 耗时{batch_cost_time}秒 | 累计写入{total_written_rows}条")
            gc.collect()

        # 打印分区写入完成总结（含总耗时）
        total_batch_time = round(total_batch_time, 2)
        print(
            f"✅ 分区{dt_partition}写入完成！累计写入{total_count}条，总耗时{total_batch_time}秒，平均每批次{round(total_batch_time / batch_num, 2)}秒")
    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败：{str(e)}")
    except Exception as e:
        raise Exception(f"分区写入异常：{str(e)}")


# ===================== 主流程 =====================
def main():
    """核心执行流程（分区并行执行+进度监控）"""
    try:
        # 记录任务总开始时间
        task_start_time = time.time()

        # 初始化日志
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"分区范围：{START_DT} ~ {END_DT} | 目标表：{TARGET_TABLE}")
        print(
            f"并行配置：活动解析={PARALLEL_CONFIG['campaign_max_workers']} | 分区写入={PARALLEL_CONFIG['partition_max_workers']} | 批次大小={PARALLEL_CONFIG['batch_size']}")
        print(f"接口配置：请求间隔={API_CONFIG['request_interval']}秒 | 保留原生数据类型输出")

        # 1. 先创建适配的表结构
        create_mz_tv_table_if_not_exists()

        # 2. 获取Token
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 3. 获取活动列表
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 4. 生成日期列表
        daily_partition_dates = get_date_range_by_start_end(START_DT, END_DT)
        print(f"✅ 生成每日分区：共{len(daily_partition_dates)}天")

        # 5. 并行解析所有分区数据（先解析，再并行写入）
        partition_data_map = {}
        print(f"\n===== 开始并行解析所有分区 =====")
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["campaign_max_workers"]) as parse_executor:
            future_to_partition = {
                parse_executor.submit(process_single_partition, token, campaign_list, dt): dt
                for dt in daily_partition_dates
            }

            for future in as_completed(future_to_partition):
                dt = future_to_partition[future]
                try:
                    partition_dt, partition_data = future.result()
                    partition_data_map[partition_dt] = partition_data
                    print(f"\n🎉 分区{partition_dt}解析完成，等待写入")
                except Exception as e:
                    print(f"❌ 分区{dt}解析失败：{str(e)}")
                    continue

        # 6. 并行写入所有分区数据
        print(f"\n===== 开始并行写入所有分区 =====")
        with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["partition_max_workers"]) as write_executor:
            future_to_write = {
                write_executor.submit(write_to_odps_partition, TARGET_TABLE, data, dt): dt
                for dt, data in partition_data_map.items()
            }

            # 等待所有分区写入完成，打印整体进度
            completed_partitions = 0
            total_partitions = len(future_to_write)
            for future in as_completed(future_to_write):
                dt = future_to_write[future]
                try:
                    future.result()
                    completed_partitions += 1
                    overall_progress = round((completed_partitions / total_partitions) * 100, 2)
                    print(f"\n📈 整体进度：{completed_partitions}/{total_partitions}个分区写入完成（{overall_progress}%）")
                except Exception as e:
                    completed_partitions += 1
                    overall_progress = round((completed_partitions / total_partitions) * 100, 2)
                    print(f"\n❌ 分区{dt}写入失败：{str(e)}")
                    print(f"📈 整体进度：{completed_partitions}/{total_partitions}个分区处理完成（{overall_progress}%）")
                    continue

        # 计算任务总耗时
        task_cost_time = round(time.time() - task_start_time, 2)
        # 任务结束
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📊 任务总结：")
        print(f"   - 总分区数：{len(daily_partition_dates)}个")
        print(f"   - 成功处理分区数：{len(partition_data_map)}个")
        print(f"   - 任务总耗时：{task_cost_time}秒")
        print(f"   - 平均每个分区耗时：{round(task_cost_time / len(daily_partition_dates), 2)}秒")
    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()