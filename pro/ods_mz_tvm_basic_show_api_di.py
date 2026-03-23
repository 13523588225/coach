# -*- coding: utf-8 -*-
import requests
import json
import time
import gc
import urllib3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
from odps import ODPS, errors
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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

# 2. ODPS配置（移除项目前缀，仅保留表名）
ODPS_PROJECT = ODPS().project
TARGET_TABLE = "ods_mz_tvm_basic_show_api_di"

# 3. 日期配置（固定单日期处理）
dt = '20260301'

# 4. 接口多维度参数（所有组合）
REPORT_PARAMS = {
    "metrics": "all",
    "by_region": ["level0", "level1", "level2"],  # 地区维度
    "by_audience": ["overall", "stable", "target"],  # 受众维度
    "platform": ["pc", "pm", "mb"],  # 平台维度
    "by_position": ["campaign", "publisher", "spot", "keyword"]  # 点位维度
}

# 5. 并行/批次配置（双层并发+全局控制）
PARALLEL_CONFIG = {
    "combo_max_workers": 10,  # 参数组合层并发数（建议≤10，避免接口限流）
    "campaign_max_workers": 10,  # 活动层并发数
    "global_semaphore": 50,  # 全局请求并发上限（总同时请求数）
    "batch_size": 20000  # ODPS写入批次大小
}

# 6. 小时粒度字段列表（h00~h23）
HOUR_FIELDS = [f"h{i:02d}" for i in range(24)]

# 7. 全局并发控制信号量（核心：限制总接口请求数）
GLOBAL_SEMAPHORE = threading.Semaphore(PARALLEL_CONFIG["global_semaphore"])


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


# ===================== 带重试+并发控制的请求函数（核心改造） =====================
@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2s→4s→8s
    retry=retry_if_exception_type(
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError))
)
def safe_request_get(url: str, params: Dict = None, timeout: int = 30) -> Dict:
    """带重试+全局并发控制的GET请求"""
    with GLOBAL_SEMAPHORE:  # 全局并发限制，获取信号量后再请求
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=timeout,
                verify=False
            )
            resp.raise_for_status()  # 非200状态码抛异常触发重试
            time.sleep(API_CONFIG["request_interval"])  # 基础请求间隔
            return resp.json()
        except Exception as e:
            print(f"❌ 接口请求失败（将重试）：{url} | 异常：{str(e)}")
            raise  # 抛出异常触发tenacity重试


@retry(
    stop=stop_after_attempt(2),  # Token获取重试2次
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type(
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError))
)
def safe_request_post(url: str, data: Dict = None, headers: Dict = None, timeout: int = 30) -> Dict:
    """带重试的POST请求（用于Token获取）"""
    try:
        resp = requests.post(
            url,
            data=data,
            headers=headers,
            timeout=timeout,
            verify=False
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"❌ POST请求失败（将重试）：{url} | 异常：{str(e)}")
        raise


# ===================== 秒针接口调用（双层并发改造） =====================
def get_miaozhen_token() -> str:
    """获取鉴权Token（带重试）"""
    try:
        token_data = safe_request_post(
            API_CONFIG["token_url"],
            data=API_CONFIG["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=API_CONFIG["timeout"]
        )
        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误：{token_data.get('error_message')}")
        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token返回为空")
        return clean_string_value(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表（带重试）"""
    try:
        campaigns_data = safe_request_get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"]
        )
        campaigns = campaigns_data.get("result", {}).get("campaigns", [])
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


def parse_single_campaign_with_params(token: str, campaign: Dict, daily_dt: str, by_region: str, by_audience: str,
                                      platform: str, by_position: str) -> Tuple[List[List[Any]], str, str, int]:
    """解析单个活动+单个参数组合的单日期数据"""
    campaign_data = []
    camp_id = campaign["campaign_id"]
    camp_start = campaign["camp_start_date"]
    camp_end = campaign["camp_end_date"]
    param_combination = f"by_region={by_region},by_audience={by_audience},platform={platform},by_position={by_position}"

    # 校验日期有效性
    if not is_date_in_campaign_valid(daily_dt, camp_start, camp_end):
        return campaign_data, camp_id, param_combination, 0

    # 转换日期格式
    report_date_10bit = date_convert(daily_dt, "10位")
    if not report_date_10bit:
        return campaign_data, camp_id, param_combination, 0

    try:
        # 构造请求参数（包含所有维度）
        request_params = {
            "campaign_id": camp_id,
            "date": report_date_10bit,
            "metrics": REPORT_PARAMS["metrics"],
            "by_region": by_region,
            "by_audience": by_audience,
            "platform": platform,
            "by_position": by_position
        }

        # 调用带重试的接口请求
        raw_data = safe_request_get(
            f"{API_CONFIG['report_basic_url']}",
            params={**request_params, "access_token": token},
            timeout=API_CONFIG["timeout"]
        )

        # 校验返回结果
        if raw_data.get("error_code") != 0:
            print(f"⚠️ 活动{camp_id}参数组合{param_combination}接口返回错误：{raw_data.get('error_message')}")
            return campaign_data, camp_id, param_combination, 0

        result = raw_data.get("result", {})
        if not all([result.get("date"), result.get("campaignId"), result.get("items")]):
            return campaign_data, camp_id, param_combination, 0

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

            # 1. 组装请求参数字段
            request_fields = [
                clean_string_value(request_params["campaign_id"]),
                clean_string_value(request_params["date"]),
                clean_string_value(request_params["metrics"]),
                clean_string_value(request_params["by_region"]),
                clean_string_value(request_params["by_audience"]),
                clean_string_value(request_params["platform"]),
                clean_string_value(request_params["by_position"])
            ]

            # 2. 组装活动基础字段
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

            # 3. 组装小时粒度曝光指标
            imp_hour_fields = [clean_numeric_value(metrics.get(f"imp_{hour}")) for hour in HOUR_FIELDS]

            # 4. 组装小时粒度点击指标
            clk_hour_fields = [clean_numeric_value(metrics.get(f"clk_{hour}")) for hour in HOUR_FIELDS]

            # 5. 组装元数据字段
            meta_fields = [
                pre_parse_raw_text,
                clean_string_value(etl_datetime)
            ]

            # 合并所有字段
            write_row = request_fields + base_fields + imp_hour_fields + clk_hour_fields + meta_fields
            campaign_data.append(write_row)

    except Exception as e:
        print(f"❌ 活动{camp_id}参数组合{param_combination}解析异常：{str(e)}")

    return campaign_data, camp_id, param_combination, len(campaign_data)


def parse_single_param_combination(token: str, campaign_list: List[Dict], daily_dt: str,
                                   combo: Tuple[str, str, str, str]) -> Tuple[List[List[Any]], str]:
    """解析单个参数组合（封装为独立函数，供上层并行调用）"""
    by_region, by_audience, platform, by_position = combo
    param_combination = f"by_region={by_region},by_audience={by_audience},platform={platform},by_position={by_position}"
    combo_data = []
    total_rows = 0

    # 活动维度并行解析
    with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["campaign_max_workers"]) as executor:
        future_to_campaign = {
            executor.submit(parse_single_campaign_with_params, token, campaign, daily_dt, by_region, by_audience,
                            platform, by_position): campaign
            for campaign in campaign_list
        }

        # 收集结果
        for future in as_completed(future_to_campaign):
            campaign = future_to_campaign[future]
            try:
                campaign_data, camp_id, _, row_count = future.result()
                if campaign_data:
                    combo_data.extend(campaign_data)
                    total_rows += row_count
            except Exception as e:
                print(f"❌ 参数组合{param_combination}下活动{campaign['campaign_id']}解析失败：{str(e)}")

    print(f"✅ 参数组合{param_combination}解析完成：新增{total_rows}条数据")
    return combo_data, param_combination


def process_single_date_optimized(token: str, campaign_list: List[Dict], daily_dt: str) -> Tuple[str, List[List[Any]]]:
    """双层并发处理单个日期：参数组合层 + 活动层并行"""
    print(
        f"\n🚀 开始处理日期：{daily_dt} | 待解析活动数：{len(campaign_list)}个 | 待遍历参数组合数：{len(REPORT_PARAMS['by_region']) * len(REPORT_PARAMS['by_audience']) * len(REPORT_PARAMS['platform']) * len(REPORT_PARAMS['by_position'])}个")
    print(
        f"🔧 并行配置：参数组合层并发={PARALLEL_CONFIG['combo_max_workers']} | 活动层并发={PARALLEL_CONFIG['campaign_max_workers']} | 全局请求上限={PARALLEL_CONFIG['global_semaphore']}")

    # 生成所有参数组合列表
    param_combinations = []
    for by_region in REPORT_PARAMS['by_region']:
        for by_audience in REPORT_PARAMS['by_audience']:
            for platform in REPORT_PARAMS['platform']:
                for by_position in REPORT_PARAMS['by_position']:
                    param_combinations.append((by_region, by_audience, platform, by_position))

    # 参数组合层并行解析（核心：双层并发）
    date_data = []
    total_combo = len(param_combinations)
    completed_combo = 0

    with ThreadPoolExecutor(max_workers=PARALLEL_CONFIG["combo_max_workers"]) as combo_executor:
        # 提交所有参数组合任务
        future_to_combo = {
            combo_executor.submit(parse_single_param_combination, token, campaign_list, daily_dt, combo): combo
            for combo in param_combinations
        }

        # 实时收集结果并打印进度
        for future in as_completed(future_to_combo):
            combo = future_to_combo[future]
            completed_combo += 1
            try:
                combo_data, combo_str = future.result()
                date_data.extend(combo_data)
                progress = round((completed_combo / total_combo) * 100, 2)
                print(
                    f"⏳ 参数组合进度：{completed_combo}/{total_combo}（{progress}%）| 当前组合：{combo_str} | 累计数据量：{len(date_data)}条")
            except Exception as e:
                completed_combo += 1
                progress = round((completed_combo / total_combo) * 100, 2)
                print(f"❌ 参数组合{combo}解析失败：{str(e)} | 进度：{completed_combo}/{total_combo}（{progress}%）")

    print(f"✅ 日期{daily_dt} - 解析完成，共生成{len(date_data)}条数据（累计解析{total_combo}个参数组合）")
    return (daily_dt, date_data)


# ===================== ODPS写入（保留原有逻辑） =====================
def write_to_odps_date(table_name: str, data: List[List[Any]], dt_partition: str):
    """按日期写入ODPS（默认分区=dt_partition，含写入进度监控）"""
    if not data:
        print(f"⚠️ 日期{dt_partition}无数据可写入，跳过")
        return

    o = ODPS(project=ODPS_PROJECT)
    # 校验表是否存在（仅提示，不创建）
    if not o.exist_table(table_name):
        raise Exception(f"❌ ODPS表{table_name}不存在，请提前创建后再执行！")

    table = o.get_table(table_name)
    # 上传参数固定为当前处理日期（默认分区）
    partition_spec = f"dt='{dt_partition}'"
    batch_size = PARALLEL_CONFIG["batch_size"]
    total_count = len(data)
    batch_num = (total_count + batch_size - 1) // batch_size  # 向上取整计算总批次

    try:
        # 清空已有分区（默认分区）
        if table.exist_partition(partition_spec):
            drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
            o.execute_sql(drop_sql)
            print(f"✅ 日期{dt_partition} - 已清空历史数据（默认分区dt={dt_partition}）")

        # 打印批次规划信息
        print(
            f"📊 日期{dt_partition} - 写入规划：总数据量{total_count}条，分{batch_num}批次（每批次{batch_size}条），上传参数默认分区dt={dt_partition}")

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

            # 写入当前批次（上传参数=默认分区dt）
            with table.open_writer(partition=partition_spec, create_partition=True) as writer:
                writer.write(batch_data)

            # 计算批次耗时（保留2位小数）
            batch_cost_time = round(time.time() - batch_start_time, 2)
            total_batch_time += batch_cost_time
            completed_batches += 1
            total_written_rows += batch_actual_count

            # 打印当前批次进度+耗时（标注默认分区）
            write_progress = round((completed_batches / batch_num) * 100, 2)
            print(
                f"🔄 日期{dt_partition} - 写入进度：{completed_batches}/{batch_num}批次（{write_progress}%）| 本批写入{batch_actual_count}条 | 耗时{batch_cost_time}秒 | 累计写入{total_written_rows}条 | 默认分区dt={dt_partition}")
            gc.collect()

        # 打印日期写入完成总结（含总耗时）
        total_batch_time = round(total_batch_time, 2)
        print(
            f"✅ 日期{dt_partition}写入完成！累计写入{total_count}条，总耗时{total_batch_time}秒，平均每批次{round(total_batch_time / batch_num, 2)}秒 | 默认分区dt={dt_partition}")
    except errors.ODPSError as e:
        raise Exception(f"ODPS写入失败（默认分区dt={dt_partition}）：{str(e)}")
    except Exception as e:
        raise Exception(f"日期写入异常（默认分区dt={dt_partition}）：{str(e)}")


# ===================== 主流程（集成双层并发+重试） =====================
def main():
    """核心执行流程（双层并发+接口重试+全局控制）"""
    try:
        # 记录任务总开始时间
        task_start_time = time.time()

        # 初始化日志
        print(f"===== 任务开始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"处理日期：{dt} | 目标表：{TARGET_TABLE}")
        print(
            f"并行配置：参数组合层={PARALLEL_CONFIG['combo_max_workers']} | 活动层={PARALLEL_CONFIG['campaign_max_workers']} | 全局请求上限={PARALLEL_CONFIG['global_semaphore']}")
        print(f"重试配置：接口请求最多重试3次 | Token获取最多重试2次 | 指数退避等待")
        print(
            f"参数组合：by_region({len(REPORT_PARAMS['by_region'])}) × by_audience({len(REPORT_PARAMS['by_audience'])}) × platform({len(REPORT_PARAMS['platform'])}) × by_position({len(REPORT_PARAMS['by_position'])}) = {len(REPORT_PARAMS['by_region']) * len(REPORT_PARAMS['by_audience']) * len(REPORT_PARAMS['platform']) * len(REPORT_PARAMS['by_position'])}个组合")

        # 1. 获取Token（带重试）
        token = get_miaozhen_token()
        print(f"✅ 秒针Token获取成功")

        # 2. 获取活动列表（带重试）
        campaign_list = get_campaign_list(token)
        if not campaign_list:
            raise Exception("❌ 活动列表为空，任务终止")

        # 3. 双层并发处理单个固定日期
        date_dt, date_data = process_single_date_optimized(token, campaign_list, dt)

        # 4. 写入ODPS（默认分区=处理日期）
        write_to_odps_date(TARGET_TABLE, date_data, date_dt)

        # 计算任务总耗时
        task_cost_time = round(time.time() - task_start_time, 2)
        # 任务结束
        print(f"\n===== 任务完成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
        print(f"📊 任务总结：")
        print(f"   - 处理日期：{dt}")
        print(
            f"   - 总参数组合数：{len(REPORT_PARAMS['by_region']) * len(REPORT_PARAMS['by_audience']) * len(REPORT_PARAMS['platform']) * len(REPORT_PARAMS['by_position'])}个")
        print(f"   - 总写入数据量：{len(date_data)}条")
        print(f"   - 任务总耗时：{task_cost_time}秒")
        print(f"   - 上传分区：dt={dt}")
    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    # 安装依赖（首次执行需运行）
    # pip install tenacity requests odps
    main()