# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置（字段名调整：start_dt/end_dt） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    # 采集日期范围：字段名改为start_dt/end_dt，避免与活动字段重名
    "start_dt": "20260101",
    "end_dt": "20260105",
    # 报表参数
    "report_params": {
        "metrics": "all",
        "by_position": "spot",
        "by_region_list": ["level0", "level1", "level2"]
    },
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "report_url": "https://api.cn.miaozhen.com/monitortv/v1/reports/basic/show",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 30,
        "interval": 0.1
    },
    "table_name": "ods_mz_adm_basic_show_api_di",
    "batch_size": 1000
}

# 自动获取ODPS项目名
try:
    ODPS_PROJECT = ODPS().project
    if not ODPS_PROJECT:
        raise Exception("ODPS项目名自动获取失败，请检查ODPS客户端配置")
    print(f"✅ 自动获取ODPS项目名：{ODPS_PROJECT}")
except Exception as e:
    raise Exception(f"ODPS初始化失败：{str(e)}")


# ===================== 工具函数 =====================
def safe_str(val):
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_date(date_ymd):
    """转换日期格式：yyyyMMdd -> yyyy-MM-dd"""
    try:
        return datetime.strptime(date_ymd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        raise Exception(f"日期格式错误：{date_ymd}（需为yyyyMMdd格式）")


def generate_date_range(start_ymd, end_ymd):
    """生成日期范围列表（yyyyMMdd）"""
    try:
        start_dt = datetime.strptime(start_ymd, "%Y%m%d")
        end_dt = datetime.strptime(end_ymd, "%Y%m%d")
        date_list = []
        current_dt = start_dt
        while current_dt <= end_dt:
            date_list.append(current_dt.strftime("%Y%m%d"))
            current_dt += timedelta(days=1)
        return date_list
    except Exception as e:
        raise Exception(f"生成日期范围失败：{str(e)}")


def check_date_overlap(campaigns, collect_start_dt, collect_end_dt):
    """
    校验采集日期范围是否与活动有效期有交集
    :param campaigns: 活动列表（含start_date/end_date）
    :param collect_start_dt: 采集开始日期（datetime）
    :param collect_end_dt: 采集结束日期（datetime）
    :return: 有交集返回True，无交集返回False
    """
    for camp in campaigns:
        try:
            # 活动字段仍为start_date/end_date，不修改
            camp_start_dt = datetime.strptime(camp["start_date"], "%Y-%m-%d")
            camp_end_dt = datetime.strptime(camp["end_date"], "%Y-%m-%d")
            # 判定是否有交集：采集范围与活动有效期重叠
            if not (collect_end_dt < camp_start_dt or collect_start_dt > camp_end_dt):
                return True
        except Exception as e:
            print(f"⚠️ 活动{camp.get('campaign_id')}日期解析失败：{str(e)}")
            continue
    return False


# ===================== 核心API函数 =====================
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
    """获取有效活动（含campaign_id/start_date/end_date）"""
    print("🔍 获取活动列表（campaign_id/start_date/end_date）...")
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
            if isinstance(camp, dict) and all([
                camp.get("campaign_id"),
                camp.get("start_date"),  # 活动字段仍为start_date
                camp.get("end_date")  # 活动字段仍为end_date
            ]):
                valid_camps.append({
                    "campaign_id": camp.get("campaign_id"),
                    "start_date": camp.get("start_date"),
                    "end_date": camp.get("end_date")
                })
        print(f"✅ 获取到{len(valid_camps)}个有效活动")
        return valid_camps
    except Exception as e:
        raise Exception(f"活动列表获取失败：{str(e)}")


def collect_report_data(token, camp_info, day_date, by_position, by_region):
    """采集单日单活动数据（校验单日是否在活动有效期内）"""
    camp_id = camp_info["campaign_id"]
    try:
        # 活动字段仍为start_date/end_date
        camp_start_dt = datetime.strptime(camp_info["start_date"], "%Y-%m-%d")
        camp_end_dt = datetime.strptime(camp_info["end_date"], "%Y-%m-%d")
        day_dt = datetime.strptime(day_date, "%Y-%m-%d")

        if day_dt < camp_start_dt or day_dt > camp_end_dt:
            print(
                f"  ⏩ 活动{camp_id}：日期[{day_date}]超出活动有效期[{camp_info['start_date']}~{camp_info['end_date']}]")
            return None
    except Exception as e:
        print(f"  ⚠️ 活动{camp_id}日期校验失败：{str(e)}")
        return None

    # 构造请求参数
    try:
        print(f"  🔍 采集活动{camp_id} | 位置[{by_position}] | 地域[{by_region}] | 日期[{day_date}]...")
        params = {
            "access_token": token,
            "campaign_id": camp_id,
            "start_date": day_date,  # 接口参数仍为start_date/end_date，不修改
            "end_date": day_date,
            "metrics": CONFIG["report_params"]["metrics"],
            "by_position": by_position,
            "by_region": by_region
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
            "day_date": day_date,
            "by_position": by_position,
            "by_region": by_region,
            "pre_parse_raw_text": resp.text,
            "parsed_data": resp.json()
        }
    except Exception as e:
        print(f"  ❌ 活动{camp_id}采集失败：{str(e)}")
        return None


# ===================== ODPS操作 =====================
def init_odps_client():
    try:
        odps = ODPS(project=ODPS_PROJECT)
        options.tunnel.use_instance_tunnel = True
        options.read_timeout = 300
        options.connect_timeout = 60
        print(f"✅ ODPS初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def assemble_report_data(reports):
    """组装数据（保留campaign字段）"""
    etl_datetime = get_etl_datetime()
    data = []
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

        row = [
            # 活动信息（仍为campaign_id/start_date/end_date）
            safe_str(camp_info["campaign_id"]),
            safe_str(camp_info["start_date"]),
            safe_str(camp_info["end_date"]),
            # 报表日期
            safe_str(report["day_date"]),
            safe_str(report["by_position"]),
            safe_str(report["by_region"]),
            safe_str(CONFIG["report_params"]["metrics"]),
            # 指标字段
        ]
        row.extend([safe_str(report_item.get(field)) for field in base_fields])
        row.append(safe_str(report["pre_parse_raw_text"]))
        row.append(etl_datetime)

        data.append(row)
    return data


def write_odps_data(odps, data, partition_dt):
    """按单日写入对应分区"""
    if not data:
        print(f"⚠️ 表{CONFIG['table_name']}分区{partition_dt}无数据可写入")
        return

    try:
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"ODPS表不存在：{CONFIG['table_name']}")

        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{partition_dt}'"

        # 清空原有分区数据
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {CONFIG['table_name']} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 分批写入
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for i in range(0, len(data), CONFIG["batch_size"]):
                batch_data = data[i:i + CONFIG["batch_size"]]
                writer.write(batch_data)
                print(f"🔸 写入批次{i // CONFIG['batch_size'] + 1} | 条数：{len(batch_data)}")

        print(f"✅ ODPS写入完成 | 总条数：{len(data)} | 分区：{partition_dt}")
    except Exception as e:
        raise Exception(f"ODPS分区{partition_dt}写入失败：{str(e)}")


# ===================== 主流程（核心调整：引用start_dt/end_dt） =====================
def main():
    print("=" * 80)
    print("🚀 秒针日报表采集任务启动（字段名调整+全局日期校验）")
    # 引用调整后的字段名start_dt/end_dt
    print(f"📅 配置采集日期范围：{CONFIG['start_dt']} ~ {CONFIG['end_dt']}")
    print("=" * 80)

    try:
        # 1. 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 2. 获取活动列表
        campaigns = get_valid_campaigns(token)
        if not campaigns:
            print("⚠️ 无有效活动，任务终止")
            return

        # 3. 全局日期校验：使用调整后的start_dt/end_dt
        collect_start_dt = datetime.strptime(CONFIG["start_dt"], "%Y%m%d")
        collect_end_dt = datetime.strptime(CONFIG["end_dt"], "%Y%m%d")
        has_overlap = check_date_overlap(campaigns, collect_start_dt, collect_end_dt)

        if not has_overlap:
            # 提示信息中使用新字段名
            print(f"❌ 配置的采集日期范围[{CONFIG['start_dt']}~{CONFIG['end_dt']}]与所有活动有效期无交集，跳过日报执行")
            return  # 直接终止任务，不执行后续采集逻辑

        # 4. 生成日期列表：传入start_dt/end_dt的值
        date_list = generate_date_range(CONFIG["start_dt"], CONFIG["end_dt"])
        print(f"\n📅 生成日期列表：{date_list}（共{len(date_list)}天）")

        # 5. 按天遍历采集
        total_collected = 0
        by_position = CONFIG["report_params"]["by_position"]
        for day_ymd in date_list:
            print(f"\n{'=' * 60}")
            print(f"📌 开始处理日期：{day_ymd}")
            print(f"{'=' * 60}")

            day_date = format_date(day_ymd)
            daily_valid_reports = []

            # 遍历地域维度+活动
            for by_region in CONFIG["report_params"]["by_region_list"]:
                print(f"\n🔹 处理地域[{by_region}]")
                for camp in campaigns:
                    report = collect_report_data(
                        token=token,
                        camp_info=camp,
                        day_date=day_date,
                        by_position=by_position,
                        by_region=by_region
                    )
                    if report:
                        daily_valid_reports.append(report)
                    time.sleep(CONFIG["api"]["interval"])

            # 写入当日分区
            if daily_valid_reports:
                daily_data = assemble_report_data(daily_valid_reports)
                write_odps_data(odps, daily_data, partition_dt=day_ymd)
                total_collected += len(daily_valid_reports)
            else:
                print(f"⚠️ 日期{day_ymd}无有效数据，跳过写入")

        # 6. 任务统计
        print("\n" + "=" * 80)
        print("✅ 任务完成！")
        print(f"📊 总有效采集数据：{total_collected}条")
        print(f"📊 生成分区数：{len([d for d in date_list if total_collected > 0])}个")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()