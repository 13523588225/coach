# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置（ODPS自动获取+固定起止日期） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    # 核心修改：固定起止日期（默认值）
    "START_DATE": "20260101",  # 开始日期固定为20260101
    "END_DATE": "20260101",  # 结束日期固定为20260101
    # 报表参数：两个参数独立配置（保持独立性）
    "report_params": {
        "metrics": "all",  # 固定参数：all
        "by_position_list": ["spot"],  # 位置维度（独立配置）
        "by_region_list": ["level0", "level1", "level2"]  # 地域维度（独立配置）
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
        "timeout": 30,
        "interval": 0.1  # 接口间隔（避免限流）
    },
    "table_name": "ods_mz_adm_admonitor_basic_show_api_di",
    "batch_size": 1000
}

# 自动获取ODPS项目名
try:
    ODPS_PROJECT = ODPS().project
    if not ODPS_PROJECT:
        raise Exception("ODPS项目名自动获取失败，请检查ODPS客户端配置")
    print(f"✅ 自动获取ODPS项目名：{ODPS_PROJECT}")
except Exception as e:
    raise Exception(f"ODPS项目初始化失败：{str(e)}")


# ===================== 内置工具函数 =====================
def safe_str(val):
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_date(date_ymd):
    """转换日期格式：yyyyMMdd -> yyyy-MM-dd"""
    try:
        return datetime.strptime(date_ymd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        raise Exception(f"日期格式错误：{date_ymd}（需为yyyyMMdd格式）")


# ===================== 核心API调用函数 =====================
def get_miaozhen_token():
    print("🔍 获取秒针Token...")
    try:
        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=CONFIG["api"]["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()
        token = token_data.get("access_token")
        if not token:
            raise Exception(f"Token为空：{json.dumps(token_data, ensure_ascii=False)}")
        print("✅ Token获取成功")
        return token
    except Exception as e:
        raise Exception(f"Token获取失败：{str(e)}")


def get_valid_campaigns(token):
    """获取有效活动（含有效期）"""
    print("🔍 获取活动列表（含有效期）...")
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()
        campaigns = raw_data.get("result", {}).get("campaigns", [])

        valid_camps = []
        for camp in campaigns:
            if isinstance(camp, dict) and all([camp.get("campaign_id"), camp.get("start_date"), camp.get("end_date")]):
                valid_camps.append({
                    "campaign_id": camp.get("campaign_id"),
                    "start_date": camp.get("start_date"),
                    "end_date": camp.get("end_date")
                })
        print(f"✅ 获取到{len(valid_camps)}个有效活动")
        return valid_camps
    except Exception as e:
        raise Exception(f"活动列表获取失败：{str(e)}")


def collect_report_data(token, camp_info, start_date, end_date, by_position, by_region):
    """
    采集单活动+单参数组合的日报数据
    :param token: 秒针Token
    :param camp_info: 活动信息
    :param start_date: 开始日期（yyyy-MM-dd）
    :param end_date: 结束日期（yyyy-MM-dd）
    :param by_position: 位置维度值
    :param by_region: 地域维度值
    :return: 采集结果
    """
    camp_id = camp_info["campaign_id"]
    # 活动有效期校验
    try:
        camp_start_dt = datetime.strptime(camp_info["start_date"], "%Y-%m-%d")
        camp_end_dt = datetime.strptime(camp_info["end_date"], "%Y-%m-%d")
        report_start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        report_end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if report_start_dt < camp_start_dt or report_end_dt > camp_end_dt:
            print(
                f"  ⏩ 活动{camp_id}：报表日期[{start_date}~{end_date}]超出活动有效期[{camp_info['start_date']}~{camp_info['end_date']}]")
            return None
    except Exception as e:
        print(f"  ⚠️ 活动{camp_id}日期校验失败：{str(e)}")
        return None

    # 构造请求参数（两个维度参数独立传递）
    try:
        print(f"  🔍 采集活动{camp_id} | 位置[{by_position}] | 地域[{by_region}] | 日期[{start_date}~{end_date}]...")
        params = {
            "access_token": token,
            "campaign_id": camp_id,
            "start_date": start_date,  # 固定开始日期
            "end_date": end_date,  # 固定结束日期
            "metrics": CONFIG["report_params"]["metrics"],  # 固定metrics=all
            "by_position": by_position,  # 位置维度（独立传参）
            "by_region": by_region  # 地域维度（独立传参）
        }

        resp = requests.get(
            CONFIG["api"]["report_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()

        return {
            "camp_info": camp_info,
            "start_date": start_date,
            "end_date": end_date,
            "by_position": by_position,
            "by_region": by_region,
            "pre_parse_raw_text": resp.text,
            "parsed_data": resp.json()
        }
    except Exception as e:
        print(f"  ❌ 活动{camp_id}采集失败：{str(e)}")
        return None


# ===================== ODPS操作函数 =====================
def init_odps_client():
    try:
        odps = ODPS(project=ODPS_PROJECT)
        options.tunnel.use_instance_tunnel = True
        options.read_timeout = 300
        options.connect_timeout = 60
        options.tunnel.limit_instance_tunnel = False
        print(f"✅ ODPS初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def assemble_report_data(reports):
    """组装日报数据（包含所有独立参数字段）"""
    etl_date = get_etl_date()
    data = []
    # 指标字段映射
    base_fields = [
        "version", "platform", "total_spot_num", "audience", "target_id",
        "publisher_id", "spot_id", "keyword_id", "region_id", "universe",
        "imp_acc", "clk_acc", "uim_acc", "ucl_acc", "imp_day", "clk_day",
        "uim_day", "ucl_day", "imp_avg_day", "clk_avg_day", "uim_avg_day",
        "ucl_avg_day", "imp_acc_h00", "imp_acc_h23", "clk_acc_h00", "clk_acc_h23"
    ]

    for report in reports:
        camp_info = report["camp_info"]
        report_item = report["parsed_data"].get("result", report["parsed_data"])
        report_item = report_item[0] if isinstance(report_item, list) and report_item else report_item

        # 核心字段（包含固定日期+独立参数）
        row = [
            # 活动基础信息
            safe_str(camp_info["campaign_id"]),
            safe_str(camp_info["start_date"]),
            safe_str(camp_info["end_date"]),
            # 报表固定日期
            safe_str(report["start_date"]),
            safe_str(report["end_date"]),
            # 独立传递的参数
            safe_str(report["by_position"]),
            safe_str(report["by_region"]),
            safe_str(CONFIG["report_params"]["metrics"]),
            # 指标字段
        ]
        # 追加指标字段值
        row.extend([safe_str(report_item.get(field)) for field in base_fields])
        # 原始数据+采集时间
        row.append(safe_str(report["pre_parse_raw_text"]))
        row.append(etl_date)

        data.append(row)
    return data


def write_odps_data(odps, data):
    """ODPS批量写入（按固定日期分区）"""
    if not data:
        print(f"⚠️ 表{CONFIG['table_name']}无数据可写入")
        return

    # 分区日期使用固定的结束日期（也可根据需求改为开始日期）
    partition_dt = CONFIG["END_DATE"]
    try:
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"ODPS表不存在：{CONFIG['table_name']}")

        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{partition_dt}'"

        # 清空原有分区数据
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {CONFIG['table_name']} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 分批写入（性能优化）
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for i in range(0, len(data), CONFIG["batch_size"]):
                batch_data = data[i:i + CONFIG["batch_size"]]
                writer.write(batch_data)
                print(f"🔸 写入批次{i // CONFIG['batch_size'] + 1} | 条数：{len(batch_data)}")

        print(f"✅ ODPS写入完成 | 总条数：{len(data)} | 分区：{partition_dt}")
    except Exception as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针日报表采集任务启动（固定起止日期版）")
    print(f"📅 固定采集日期：{CONFIG['START_DATE']} ~ {CONFIG['END_DATE']}")
    print(f"🔧 报表参数：metrics={CONFIG['report_params']['metrics']}")
    print(f"🔧 位置维度：{CONFIG['report_params']['by_position_list']}")
    print(f"🔧 地域维度：{CONFIG['report_params']['by_region_list']}")
    print("=" * 80)

    try:
        # 1. 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 2. 转换固定日期格式（yyyyMMdd -> yyyy-MM-dd）
        start_date = format_date(CONFIG["START_DATE"])
        end_date = format_date(CONFIG["END_DATE"])

        # 3. 获取有效活动列表
        campaigns = get_valid_campaigns(token)

        # 4. 遍历所有参数组合（位置+地域维度独立遍历）
        valid_reports = []
        for by_position in CONFIG["report_params"]["by_position_list"]:
            for by_region in CONFIG["report_params"]["by_region_list"]:
                print(f"\n📌 开始采集 | 位置[{by_position}] | 地域[{by_region}]")
                for camp in campaigns:
                    report = collect_report_data(token, camp, start_date, end_date, by_position, by_region)
                    if report:
                        valid_reports.append(report)
                    time.sleep(CONFIG["api"]["interval"])  # 接口间隔，避免限流

        # 5. 组装数据并写入ODPS
        if valid_reports:
            data = assemble_report_data(valid_reports)
            write_odps_data(odps, data)
        else:
            print("⚠️ 无有效报表数据，跳过ODPS写入")

        # 6. 任务完成统计
        print("\n" + "=" * 80)
        print("✅ 日报表采集任务完成！")
        print(f"📊 有效采集数据：{len(valid_reports)}条")
        print(
            f"📊 覆盖参数组合：{len(CONFIG['report_params']['by_position_list'])}×{len(CONFIG['report_params']['by_region_list'])}={len(CONFIG['report_params']['by_position_list']) * len(CONFIG['report_params']['by_region_list'])}种")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise  # 抛出异常，便于调度平台捕获


if __name__ == "__main__":
    main()