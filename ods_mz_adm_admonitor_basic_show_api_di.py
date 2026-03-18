# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置 =====================
# 关闭SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. ODPS项目自动获取
ODPS_PROJECT = ODPS().project
# 2. 固定采集日期（默认值）
START_DATE = "20260101"  # 开始日期固定为20260101
END_DATE = "20260101"  # 结束日期固定为20260101
# 3. 核心配置
CONFIG = {
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
        "interval": 0.2
    },
    "tables": {
        "campaign": "ods_mz_adm_cms_campaigns_list_api_df",
        "report": "ods_mz_adm_admonitor_basic_show_api_di"
    }
}


# ===================== 工具函数 =====================
def get_date_list(start_ymd, end_ymd):
    """生成日期列表（yyyy-MM-dd）"""
    dates = []
    current = datetime.strptime(start_ymd, "%Y%m%d")
    end = datetime.strptime(end_ymd, "%Y%m%d")
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def safe_str(val):
    """安全转换字符串，空值返回None"""
    return str(val) if val and val not in ("", "null", "undefined") else None


def get_etl_date():
    """获取数据落地时间（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 接口调用 =====================
def get_token():
    """获取接口Token"""
    print("🔍 获取接口Token...")
    try:
        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=CONFIG["api"]["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise Exception("Token为空")
        print("✅ Token获取成功")
        return token
    except Exception as e:
        raise Exception(f"Token获取失败：{e}")


def get_campaigns(token):
    """获取活动列表（适配建表字段）"""
    print("\n🔍 采集活动列表...")
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        # 保存原始响应文本
        pre_parse_raw_text = resp.text
        raw_data = resp.json()
        campaigns = raw_data.get("result", {}).get("campaigns", raw_data)
        valid_camps = []

        # 过滤有效活动并补充原始文本
        for camp in campaigns:
            if isinstance(camp, dict) and camp.get("campaign_id"):
                camp["pre_parse_raw_text"] = pre_parse_raw_text
                valid_camps.append(camp)

        print(f"✅ 采集到{len(valid_camps)}个有效活动")
        return valid_camps
    except Exception as e:
        raise Exception(f"活动采集失败：{e}")


def get_report(token, camp_id, report_date, camp_start, camp_end):
    """获取日报数据（适配建表字段）"""
    # 日期校验
    try:
        check_dt = datetime.strptime(report_date, "%Y-%m-%d")
        start_dt = datetime.strptime(camp_start, "%Y-%m-%d")
        end_dt = datetime.strptime(camp_end, "%Y-%m-%d")
        if not (start_dt <= check_dt <= end_dt):
            print(f"  ⏩ 活动{camp_id}：{report_date}不在有效期")
            return None
    except:
        print(f"  ⚠️ 活动{camp_id}：日期格式错误")
        return None

    # 调用接口
    try:
        resp = requests.get(
            f"{CONFIG['api']['report_url']}?access_token={token}",
            params={"campaign_id": camp_id, "date": report_date},
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        # 保存原始响应和解析后数据
        report_data = {
            "pre_parse_raw_text": resp.text,
            "parsed_data": resp.json(),
            "camp_start": camp_start,
            "camp_end": camp_end
        }
        return report_data
    except Exception as e:
        print(f"  ❌ 活动{camp_id} {report_date}：{e}")
        return None


# ===================== ODPS操作 =====================
def init_odps():
    """初始化ODPS客户端"""
    odps = ODPS(project=ODPS_PROJECT)
    if not odps.project:
        raise Exception("未检测到有效MaxCompute项目名！请检查DataWorks配置")

    options.tunnel.use_instance_tunnel = True
    options.read_timeout = 300
    options.connect_timeout = 60

    print(f"✅ ODPS客户端初始化成功 | 关联项目：{odps.project}")
    return odps


def write_to_mc(odps, table_name: str, data: list, dt: str):
    """写入ODPS（清空分区+创建分区）"""
    if not data:
        print(f"⚠️ {table_name} 无数据可写入")
        return

    if not odps.exist_table(table_name):
        raise Exception(f"表{table_name}不存在")

    table = odps.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    # 清空分区防重复
    if table.exist_partition(partition_spec):
        odps.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{table_name}.{partition_spec}")

    # 写入数据
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)

    print(f"✅ 写入成功：{table_name} | 分区{dt} | 条数{len(data)}")


# ===================== 主流程 =====================
def main():
    # 固定采集日期，无需参数解析
    print("=" * 60)
    print(f"秒针数据采集 | 固定日期：{START_DATE} ~ {END_DATE}")
    print("=" * 60)

    try:
        # 初始化ODPS和Token
        odps = init_odps()
        token = get_token()

        # 1. 采集活动数据（严格匹配建表字段）
        campaigns = get_campaigns(token)
        camp_data = []
        current_dt = datetime.now().strftime("%Y%m%d")  # 分区字段
        etl_date = get_etl_date()

        for camp in campaigns:
            # 严格按建表字段顺序组装数据
            camp_row = [
                safe_str(camp.get("campaign_id")),  # 0: campaign_id
                safe_str(camp.get("start_date")),  # 1: start_date
                safe_str(camp.get("end_date")),  # 2: end_date
                safe_str(camp.get("advertiser_name")),  # 3: advertiser_name
                safe_str(camp.get("agency_name")),  # 4: agency_name
                safe_str(camp.get("brand_name")),  # 5: brand_name
                safe_str(camp.get("calculation_type")),  # 6: calculation_type
                safe_str(camp.get("campaign_name")),  # 7: campaign_name
                safe_str(camp.get("campaign_type")),  # 8: campaign_type
                safe_str(camp.get("creator_name")),  # 9: creator_name
                safe_str(camp.get("description")),  # 10: description
                safe_str(camp.get("linked_iplib")),  # 11: linked_iplib
                safe_str(camp.get("linked_panels")),  # 12: linked_panels
                safe_str(camp.get("linked_siteids")),  # 13: linked_siteids
                safe_str(camp.get("slot_type")),  # 14: slot_type
                safe_str(camp.get("pre_parse_raw_text")),  # 15: pre_parse_raw_text
                etl_date  # 16: etl_date
            ]
            camp_data.append(camp_row)

        # 写入活动表
        write_to_mc(odps, CONFIG["tables"]["campaign"], camp_data, current_dt)

        # 2. 采集日报数据（严格匹配建表字段）
        print("\n🔍 采集日报数据...")
        report_dates = get_date_list(START_DATE, END_DATE)  # 使用固定日期
        total_report = 0

        for report_date in report_dates:
            print(f"\n--- 处理日期：{report_date} ---")
            daily_report = []
            report_dt = report_date.replace("-", "")  # 分区字段（yyyyMMdd）

            for camp in campaigns:
                camp_id = camp.get("campaign_id")
                camp_start = camp.get("start_date")
                camp_end = camp.get("end_date")

                if not all([camp_id, camp_start, camp_end]):
                    continue

                # 获取日报数据
                report_result = get_report(token, camp_id, report_date, camp_start, camp_end)
                if not report_result:
                    time.sleep(CONFIG["api"]["interval"])
                    continue

                # 解析日报数据
                report_item = report_result["parsed_data"].get("result", report_result["parsed_data"])
                if isinstance(report_item, list) and len(report_item) > 0:
                    report_item = report_item[0]  # 兼容列表格式

                # 严格按建表字段顺序组装数据
                report_row = [
                    safe_str(camp_id),  # 0: campaign_id
                    safe_str(camp_start),  # 1: start_date
                    safe_str(camp_end),  # 2: end_date
                    safe_str(report_date),  # 3: date
                    safe_str(report_item.get("version")),  # 4: version
                    safe_str(report_item.get("platform")),  # 5: platform
                    safe_str(report_item.get("total_spot_num")),  # 6: total_spot_num
                    safe_str(report_item.get("audience")),  # 7: audience
                    safe_str(report_item.get("target_id")),  # 8: target_id
                    safe_str(report_item.get("publisher_id")),  # 9: publisher_id
                    safe_str(report_item.get("spot_id")),  # 10: spot_id
                    safe_str(report_item.get("keyword_id")),  # 11: keyword_id
                    safe_str(report_item.get("region_id")),  # 12: region_id
                    safe_str(report_item.get("universe")),  # 13: universe
                    safe_str(report_item.get("imp_acc")),  # 14: imp_acc
                    safe_str(report_item.get("clk_acc")),  # 15: clk_acc
                    safe_str(report_item.get("uim_acc")),  # 16: uim_acc
                    safe_str(report_item.get("ucl_acc")),  # 17: ucl_acc
                    safe_str(report_item.get("imp_day")),  # 18: imp_day
                    safe_str(report_item.get("clk_day")),  # 19: clk_day
                    safe_str(report_item.get("uim_day")),  # 20: uim_day
                    safe_str(report_item.get("ucl_day")),  # 21: ucl_day
                    safe_str(report_item.get("imp_avg_day")),  # 22: imp_avg_day
                    safe_str(report_item.get("clk_avg_day")),  # 23: clk_avg_day
                    safe_str(report_item.get("uim_avg_day")),  # 24: uim_avg_day
                    safe_str(report_item.get("ucl_avg_day")),  # 25: ucl_avg_day
                    safe_str(report_item.get("imp_acc_h00")),  # 26: imp_acc_h00
                    safe_str(report_item.get("imp_acc_h23")),  # 27: imp_acc_h23
                    safe_str(report_item.get("clk_acc_h00")),  # 28: clk_acc_h00
                    safe_str(report_item.get("clk_acc_h23")),  # 29: clk_acc_h23
                    safe_str(report_result["pre_parse_raw_text"]),  # 30: pre_parse_raw_text
                    get_etl_date()  # 31: etl_date
                ]
                daily_report.append(report_row)
                total_report += 1
                time.sleep(CONFIG["api"]["interval"])

            # 写入日报表
            write_to_mc(odps, CONFIG["tables"]["report"], daily_report, report_dt)

        # 任务完成
        print("\n" + "=" * 60)
        print(f"✅ 采集完成！活动{len(camp_data)}条 | 日报{total_report}条")
        print(f"📌 活动表分区：{current_dt}")
        print(f"📌 日报表分区：{START_DATE}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 任务失败：{e}")
        raise


if __name__ == "__main__":
    main()