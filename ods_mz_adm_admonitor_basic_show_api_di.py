# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import time
import argparse
import urllib3
from typing import Dict, List, Optional
from odps import ODPS, options

# ===================== 基础配置 =====================
# 关闭SSL安全警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

# 1. DataWorks中可留空（自动使用当前工作空间项目名），也可手动指定
MC_PROJECT = ""  # 示例："prod_mz_data"
# 2. 接口配置
API_CONFIG = {
    "token_url": "https://api.cn.miaozhen.com/oauth/token",
    "campaign_list_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
    "report_basic_url": "https://api.cn.miaozhen.com/admonitor/v1/reports/basic/show",
    "auth": {
        "grant_type": "password",
        "username": "Coach_api",
        "password": "Coachapi2026",
        "client_id": "COACH2026_API",
        "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
    },
    "timeout": 30,
    "request_interval": 0.2
}
# 3. 数据表名配置
TABLE_NAMES = {
    "campaign": "ods_mz_adm_cms_campaigns_list_api_df",  # 活动数据表
    "report": "ods_mz_adm_admonitor_reports_basic_api_di"  # 日报数据表
}


# ===================== 命令行参数解析 =====================
def parse_args():
    """解析采集日期参数"""
    parser = argparse.ArgumentParser(description='秒针数据采集入库工具（DataWorks适配）')
    parser.add_argument('--start_date', type=str, required=True, help='采集开始日期（yyyyMMdd）')
    parser.add_argument('--end_date', type=str, required=True, help='采集结束日期（yyyyMMdd）')
    args = parser.parse_args()

    # 日期校验
    date_format = "%Y%m%d"
    for arg_name, date_str in [("start_date", args.start_date), ("end_date", args.end_date)]:
        try:
            datetime.strptime(date_str, date_format)
        except ValueError:
            raise ValueError(f"日期格式错误！{arg_name}={date_str}，请使用{date_format}格式")

    if datetime.strptime(args.start_date, date_format) > datetime.strptime(args.end_date, date_format):
        raise ValueError("开始日期不能晚于结束日期")

    return args


# ===================== 工具函数 =====================
def date_range(start_ymd: str, end_ymd: str) -> List[str]:
    """生成日期列表（yyyy-MM-dd）"""
    start_dt = datetime.strptime(start_ymd, "%Y%m%d")
    end_dt = datetime.strptime(end_ymd, "%Y%m%d")
    dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    return dates


def get_dt_str(date_str: str) -> str:
    """yyyy-MM-dd 转 yyyyMMdd"""
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")


def get_etl_time() -> str:
    """获取ETL时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def is_date_in_range(check_date: str, start_date: str, end_date: str) -> bool:
    """校验日期是否在活动有效期内"""
    try:
        check_dt = datetime.strptime(check_date, "%Y-%m-%d")
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        return start_dt <= check_dt <= end_dt
    except Exception as e:
        print(f"⚠️ 日期校验失败：{e} | 校验日期={check_date}, 活动开始={start_date}, 活动结束={end_date}")
        return False


def safe_get(data: Dict, key: str) -> Optional[str]:
    """安全获取字典值，空值返回None"""
    val = data.get(key)
    return str(val) if val is not None and val != "" else None


# ===================== MaxCompute客户端配置 =====================
def get_odps_client() -> ODPS:
    """初始化MaxCompute客户端（DataWorks自动注入上下文）"""
    # DataWorks自动注入access_id/access_key/endpoint/project
    odps_client = ODPS()

    # 手动指定项目名（优先级高于DataWorks默认）
    if MC_PROJECT:
        odps_client.project = MC_PROJECT

    # 验证项目名有效性
    if not odps_client.project:
        raise Exception("未检测到有效MaxCompute项目名！请检查DataWorks工作空间配置或MC_PROJECT参数")

    # 开启批量写入优化
    options.tunnel.use_instance_tunnel = True
    options.tunnel.limit_instance_tunnel = False

    print(f"✅ MaxCompute客户端初始化成功 | 关联项目：{odps_client.project}")
    return odps_client


# ===================== 数据入库逻辑 =====================
def write_to_mc(odps_client: ODPS, table_name: str, records: List[Dict], partition_val: str):
    """
    写入MaxCompute表（分区表）
    :param odps_client: ODPS客户端实例
    :param table_name: 目标表名
    :param records: 待写入数据列表
    :param partition_val: 分区值（yyyyMMdd）
    """
    if not records:
        print(f"⚠️ {table_name} 无数据可写入（分区：{partition_val}）")
        return

    try:
        # 获取表对象
        table = odps_client.get_table(table_name)
        # 构造分区Spec
        partition_spec = f"dt='{partition_val}'"

        # 批量写入数据（覆盖分区，保证数据一致性）
        with table.open_writer(partition=partition_spec, overwrite=True) as writer:
            writer.write(records)

        # 打印入库日志
        print(f"✅ 成功写入{table_name} | 分区：{partition_val} | 数据条数：{len(records)}")
    except Exception as e:
        raise Exception(f"❌ 写入{table_name}失败（分区：{partition_val}）：{str(e)}")


# ===================== 接口调用逻辑 =====================
def get_api_token() -> str:
    """获取秒针接口Token"""
    print("🔍 正在请求接口Token...")
    try:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.post(
            url=API_CONFIG["token_url"],
            data=API_CONFIG["auth"],
            headers=headers,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        token_data = response.json()
        token = token_data.get("access_token")
        if not token:
            raise Exception(f"Token为空，响应数据：{json.dumps(token_data, ensure_ascii=False)}")

        print("✅ Token获取成功")
        return token
    except Exception as e:
        raise Exception(f"❌ 获取Token失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """获取活动列表原始数据（URL传Token）"""
    print("\n🔍 正在采集活动列表数据...")
    try:
        # URL拼接Token
        request_url = f"{API_CONFIG['campaign_list_url']}?access_token={token}"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response = requests.get(
            url=request_url,
            headers=headers,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        raw_data = response.json() if response.text else []
        print(f"✅ 活动列表采集完成，共{len(raw_data)}条原始数据")
        return raw_data
    except Exception as e:
        raise Exception(f"❌ 采集活动列表失败：{str(e)}")


def format_campaign_data(raw_campaign: Dict) -> Dict:
    """格式化活动数据（适配MaxCompute表结构）"""
    current_dt = get_dt_str(datetime.now().strftime("%Y-%m-%d"))
    return {
        "campaign_id": safe_get(raw_campaign, "campaign_id"),
        "start_date": safe_get(raw_campaign, "start_date"),
        "end_date": safe_get(raw_campaign, "end_date"),
        "advertiser_name": safe_get(raw_campaign, "advertiser_name"),
        "agency_name": safe_get(raw_campaign, "agency_name"),
        "brand_name": safe_get(raw_campaign, "brand_name"),
        "campaign_name": safe_get(raw_campaign, "campaign_name"),
        "campaign_type": safe_get(raw_campaign, "campaign_type"),
        "creator_name": safe_get(raw_campaign, "creator_name"),
        "description": safe_get(raw_campaign, "description"),
        "linked_iplib": safe_get(raw_campaign, "linked_iplib"),
        "linked_panels": safe_get(raw_campaign, "linked_panels"),
        "linked_siteids": safe_get(raw_campaign, "linked_siteids"),
        "slot_type": safe_get(raw_campaign, "slot_type"),
        "etl_time": get_etl_time(),
        "dt": current_dt  # 分区字段（当日）
    }


def get_report_data(token: str, campaign_id: str, report_date: str, campaign: Dict) -> Optional[Dict]:
    """获取并格式化日报数据（适配MaxCompute表结构）"""
    # 1. 校验活动有效期
    camp_start = safe_get(campaign, "start_date")
    camp_end = safe_get(campaign, "end_date")
    if not camp_start or not camp_end:
        print(f"  ⚠️ 跳过活动{campaign_id}：缺少开始/结束日期")
        return None
    if not is_date_in_range(report_date, camp_start, camp_end):
        print(f"  ⚠️ 跳过活动{campaign_id}：{report_date}不在有效期[{camp_start}, {camp_end}]")
        return None

    # 2. 调用日报接口（URL传Token）
    try:
        request_url = f"{API_CONFIG['report_basic_url']}?access_token={token}"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        params = {"campaign_id": campaign_id, "date": report_date}

        response = requests.get(
            url=request_url,
            headers=headers,
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()
        raw_report = response.json() or {}

        # 3. 格式化日报数据
        report_dt = get_dt_str(report_date)
        return {
            "campaign_id": campaign_id,
            "report_date": report_date,
            "version": safe_get(raw_report, "version"),
            "platform": safe_get(raw_report, "platform"),
            "total_spot_num": safe_get(raw_report, "total_spot_num"),
            "audience": safe_get(raw_report, "audience"),
            "target_id": safe_get(raw_report, "target_id"),
            "publisher_id": safe_get(raw_report, "publisher_id"),
            "spot_id": safe_get(raw_report, "spot_id"),
            "keyword_id": safe_get(raw_report, "keyword_id"),
            "region_id": safe_get(raw_report, "region_id"),
            "imp_acc": safe_get(raw_report, "imp_acc"),
            "clk_acc": safe_get(raw_report, "clk_acc"),
            "uim_acc": safe_get(raw_report, "uim_acc"),
            "ucl_acc": safe_get(raw_report, "ucl_acc"),
            "imp_day": safe_get(raw_report, "imp_day"),
            "clk_day": safe_get(raw_report, "clk_day"),
            "uim_day": safe_get(raw_report, "uim_day"),
            "ucl_day": safe_get(raw_report, "ucl_day"),
            "imp_avg_day": safe_get(raw_report, "imp_avg_day"),
            "clk_avg_day": safe_get(raw_report, "clk_avg_day"),
            "uim_avg_day": safe_get(raw_report, "uim_avg_day"),
            "ucl_avg_day": safe_get(raw_report, "ucl_avg_day"),
            "imp_acc_h00": safe_get(raw_report, "imp_acc_h00"),
            "imp_acc_h23": safe_get(raw_report, "imp_acc_h23"),
            "clk_acc_h00": safe_get(raw_report, "clk_acc_h00"),
            "clk_acc_h23": safe_get(raw_report, "clk_acc_h23"),
            "etl_time": get_etl_time(),
            "dt": report_dt  # 分区字段（日报日期）
        }
    except Exception as e:
        print(f"  ❌ 获取活动{campaign_id} {report_date}日报失败：{str(e)}")
        return None


# ===================== 主函数（DataWorks入口） =====================
def main():
    # 1. 初始化参数
    args = parse_args()
    start_date = args.start_date
    end_date = args.end_date
    report_dates = date_range(start_date, end_date)

    # 打印任务信息
    print("=" * 80)
    print("秒针数据采集入库工具（DataWorks适配版）")
    print(f"执行时间：{get_etl_time()}")
    print(f"采集日期范围：{start_date} ~ {end_date}（共{len(report_dates)}天）")
    print(f"活动数据表：{TABLE_NAMES['campaign']}")
    print(f"日报数据表：{TABLE_NAMES['report']}")
    print("=" * 80)

    try:
        # 2. 初始化MaxCompute客户端
        print("\n【步骤1/4】初始化MaxCompute客户端")
        odps_client = get_odps_client()

        # 3. 获取接口Token
        print("\n【步骤2/4】获取秒针接口Token")
        token = get_api_token()

        # 4. 采集并入库活动数据
        print("\n【步骤3/4】采集活动列表数据并入库")
        raw_campaigns = get_campaign_list(token)
        if raw_campaigns:
            # 格式化活动数据
            formatted_campaigns = [
                format_campaign_data(camp)
                for camp in raw_campaigns
                if safe_get(camp, "campaign_id")
            ]
            # 写入活动表（按当日分区）
            campaign_dt = get_dt_str(datetime.now().strftime("%Y-%m-%d"))
            write_to_mc(
                odps_client=odps_client,
                table_name=TABLE_NAMES["campaign"],
                records=formatted_campaigns,
                partition_val=campaign_dt
            )
        else:
            print("⚠️ 未采集到任何活动数据，跳过活动入库")
            return

        # 5. 采集并入库日报数据（按日期分分区入库）
        print("\n【步骤4/4】采集日报数据并入库")
        total_report_count = 0
        for report_date in report_dates:
            print(f"\n--- 处理日期：{report_date} ---")
            daily_report = []
            for camp in raw_campaigns:
                camp_id = safe_get(camp, "campaign_id")
                if not camp_id:
                    continue

                # 获取单条日报数据
                report_data = get_report_data(token, camp_id, report_date, camp)
                if report_data:
                    daily_report.append(report_data)

                # 接口限流
                time.sleep(API_CONFIG["request_interval"])

            # 写入当日日报分区
            if daily_report:
                report_dt = get_dt_str(report_date)
                write_to_mc(
                    odps_client=odps_client,
                    table_name=TABLE_NAMES["report"],
                    records=daily_report,
                    partition_val=report_dt
                )
                total_report_count += len(daily_report)

        # 6. 任务完成
        print("\n" + "=" * 80)
        print("✅ 数据采集入库任务完成！")
        print(f"📊 统计：活动数据{len(formatted_campaigns)}条 | 日报数据{total_report_count}条")
        print(f"📌 活动表分区：{campaign_dt}")
        print(f"📌 日报表分区：{start_date} ~ {end_date}")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise  # 抛出异常，让DataWorks标记任务失败


if __name__ == "__main__":
    main()