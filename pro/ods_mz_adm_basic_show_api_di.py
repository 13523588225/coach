# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置（MaxCompute + 日期参数） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 核心修改：自动获取ODPS项目名
ODPS_PROJECT = ODPS().project  # 从odps配置文件自动读取默认项目名

CONFIG = {
    "start_dt": "20260309",  # 配置开始日期（yyyyMMdd）
    "end_dt": "20260311",  # 配置结束日期（yyyyMMdd）
    # MaxCompute配置（使用自动获取的项目名）
    "odps": {
        "project": ODPS_PROJECT,  # 自动填充项目名
        "table_name": "ods_mz_admonitor_basic_report_df",  # 目标表名（需提前创建）
        "partition_col": "dt",  # 分区字段（yyyyMMdd）
        "batch_size": 1000  # 批量写入大小
    },
    # 报表参数：适配新接口参数
    "report_params": {
        "metrics": "all",
        "by_region_list": ["level0", "level1", "level2"]
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
    }
}


# ===================== 内置工具函数 =====================
def safe_str(val):
    """安全转换字符串，空值返回None"""
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    return str(val)


def get_etl_datetime():
    """获取当前采集时间（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_date(date_ymd):
    """转换日期格式：yyyyMMdd -> yyyy-MM-dd"""
    try:
        return datetime.strptime(date_ymd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        raise Exception(f"日期格式错误：{date_ymd}（需为yyyyMMdd格式）")


def get_date_range(start_date, end_date):
    """生成日期区间内的所有单日列表（yyyy-MM-dd）"""
    date_list = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    current_dt = start_dt
    while current_dt <= end_dt:
        date_list.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    return date_list


def is_date_overlap(config_start, config_end, camp_start, camp_end):
    """校验两个日期区间是否有交集"""
    try:
        config_start_dt = datetime.strptime(config_start, "%Y-%m-%d")
        config_end_dt = datetime.strptime(config_end, "%Y-%m-%d")
        camp_start_dt = datetime.strptime(camp_start, "%Y-%m-%d")
        camp_end_dt = datetime.strptime(camp_end, "%Y-%m-%d")

        # 无交集条件：配置结束 < 活动开始 或 配置开始 > 活动结束
        if config_end_dt < camp_start_dt or config_start_dt > camp_end_dt:
            return False
        return True
    except Exception as e:
        print(f"⚠️ 日期区间校验失败：{str(e)}")
        return False


def get_partition_dt(date_str):
    """转换日期为分区格式：yyyy-MM-dd -> yyyyMMdd"""
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")


# ===================== MaxCompute核心函数 =====================
def init_odps_client():
    """初始化ODPS客户端（使用自动获取的项目名）"""
    try:
        # 直接使用已获取的项目名初始化
        odps = ODPS(project=ODPS_PROJECT)

        # ODPS优化配置（提升写入效率）
        options.tunnel.use_instance_tunnel = True
        options.read_timeout = 300
        options.connect_timeout = 60
        options.tunnel.limit_instance_tunnel = False

        print(f"✅ ODPS客户端初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def write_data_to_odps(odps, data, partition_dt):
    """
    写入数据到MaxCompute指定分区（目标表需提前创建）
    :param odps: ODPS客户端实例
    :param data: 待写入数据（List[List]格式）
    :param partition_dt: 分区值（yyyyMMdd）
    """
    if not data:
        print(f"⚠️ 分区{partition_dt}无数据可写入，跳过")
        return

    table_name = CONFIG["odps"]["table_name"]
    partition_spec = f"{CONFIG['odps']['partition_col']}='{partition_dt}'"

    try:
        # 校验目标表是否存在
        if not odps.exist_table(table_name):
            raise Exception(f"目标表{table_name}不存在，请提前创建！")

        # 清空当日分区（避免重复数据）
        table = odps.get_table(table_name)
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 批量写入数据（按配置的batch_size分批次）
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            batch_size = CONFIG["odps"]["batch_size"]
            for i in range(0, len(data), batch_size):
                batch_data = data[i:i + batch_size]
                writer.write(batch_data)
                print(f"🔸 分区{partition_dt} | 写入批次{i // batch_size + 1} | 条数：{len(batch_data)}")

        print(f"✅ 分区{partition_dt}写入完成 | 总条数：{len(data)}")
    except Exception as e:
        raise Exception(f"写入ODPS分区{partition_dt}失败：{str(e)}")


# ===================== 秒针API采集函数 =====================
def get_miaozhen_token():
    """获取秒针接口Token"""
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
    """获取有效活动列表（含ID+有效期，用于日期校验）"""
    print("🔍 获取有效活动列表（含有效期）...")
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        if not isinstance(raw_data, list):
            raise Exception(f"活动列表返回格式异常（非数组），原始数据：{json.dumps(raw_data, ensure_ascii=False)}")

        valid_camps = []
        for idx, camp in enumerate(raw_data):
            if not isinstance(camp, dict):
                print(f"⚠️ 第{idx + 1}条活动数据非字典格式，跳过：{camp}")
                continue

            # 提取核心字段
            camp_id = camp.get("campaign_id")
            camp_start = camp.get("start_date")
            camp_end = camp.get("end_date")

            # 校验字段完整性
            if not camp_id or not camp_start or not camp_end:
                print(f"⚠️ 第{idx + 1}条活动数据核心字段缺失，跳过：{camp.get('campaign_id', '未知ID')}")
                continue

            # 校验日期格式
            try:
                datetime.strptime(camp_start, "%Y-%m-%d")
                datetime.strptime(camp_end, "%Y-%m-%d")
            except Exception as e:
                print(f"⚠️ 活动{camp_id}日期格式错误，跳过：{str(e)}")
                continue

            valid_camps.append({
                "campaign_id": str(camp_id).strip(),
                "start_date": camp_start,
                "end_date": camp_end
            })

        print(f"✅ 获取到{len(valid_camps)}个有效活动（含有效期）")
        for camp in valid_camps:
            print(f"   - 活动{camp['campaign_id']} | 有效期：{camp['start_date']}~{camp['end_date']}")
        return valid_camps
    except Exception as e:
        raise Exception(f"活动列表获取失败：{str(e)}")


def collect_report_data(token, campaign_id, single_date, by_region):
    """采集单活动+单日期+单地域维度的报表数据"""
    try:
        print(f"  🔍 采集活动{campaign_id} | 地域[{by_region}] | 日期[{single_date}]...")
        params = {
            "access_token": token,
            "campaign_id": campaign_id,
            "date": single_date,  # 新接口参数：单日日期
            "metrics": CONFIG["report_params"]["metrics"],
            "by_region": by_region
        }

        resp = requests.get(
            CONFIG["api"]["report_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        parsed_data = resp.json()

        # 组装单条数据（与ODPS表字段一一对应）
        report_item = parsed_data.get("result", parsed_data)
        report_item = report_item[0] if isinstance(report_item, list) and report_item else report_item

        row = [
            safe_str(campaign_id),  # campaign_id
            safe_str(single_date),  # report_date
            safe_str(by_region),  # by_region
            safe_str(CONFIG["report_params"]["metrics"]),  # metrics
            safe_str(report_item.get("version")),  # version
            safe_str(report_item.get("platform")),  # platform
            safe_str(report_item.get("total_spot_num")),  # total_spot_num
            safe_str(report_item.get("audience")),  # audience
            safe_str(report_item.get("target_id")),  # target_id
            safe_str(report_item.get("publisher_id")),  # publisher_id
            safe_str(report_item.get("spot_id")),  # spot_id
            safe_str(report_item.get("region_id")),  # region_id
            safe_str(report_item.get("universe")),  # universe
            safe_str(report_item.get("imp_acc")),  # imp_acc
            safe_str(report_item.get("clk_acc")),  # clk_acc
            safe_str(report_item.get("uim_acc")),  # uim_acc
            safe_str(report_item.get("ucl_acc")),  # ucl_acc
            safe_str(report_item.get("imp_day")),  # imp_day
            safe_str(report_item.get("clk_day")),  # clk_day
            safe_str(resp.text),  # pre_parse_raw_text
            get_etl_datetime()  # etl_datetime
        ]

        print(f"  ✅ 活动{campaign_id}日期[{single_date}]采集成功")
        return row
    except Exception as e:
        print(f"  ❌ 活动{campaign_id}日期[{single_date}]采集失败：{str(e)}")
        return None


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针日报表采集任务启动（写入MaxCompute + 单日分区）")
    print(f"📅 配置日期区间：{CONFIG['start_dt']} ~ {CONFIG['end_dt']}")
    print(f"🔧 ODPS项目：{ODPS_PROJECT}")
    print(f"🔧 目标表：{CONFIG['odps']['table_name']}")
    print(f"🔧 报表参数：metrics={CONFIG['report_params']['metrics']}")
    print(f"🔧 地域维度：{CONFIG['report_params']['by_region_list']}")
    print("=" * 80)

    try:
        # 1. 校验ODPS项目名是否获取成功
        if not ODPS_PROJECT:
            raise Exception("ODPS项目名自动获取失败，请检查~/.odps_config.ini配置")

        # 2. 初始化ODPS客户端（移除建表逻辑）
        odps = init_odps_client()

        # 3. 获取秒针Token
        token = get_miaozhen_token()

        # 4. 转换配置日期格式
        config_start = format_date(CONFIG["start_dt"])
        config_end = format_date(CONFIG["end_dt"])

        # 5. 获取有效活动列表（含有效期）
        valid_camps = get_valid_campaigns(token)
        if not valid_camps:
            print("⚠️ 未获取到任何有效活动，任务终止")
            return

        # 6. 按单日分区存储数据（核心逻辑）
        date_data_map = {}  # key: 分区日期(yyyyMMdd), value: 待写入数据列表

        for camp in valid_camps:
            camp_id = camp["campaign_id"]
            camp_start = camp["start_date"]
            camp_end = camp["end_date"]

            # 日期交集校验：无交集则跳过
            if not is_date_overlap(config_start, config_end, camp_start, camp_end):
                print(f"\n⏩ 活动{camp_id}：配置区间与活动有效期无交集，跳过")
                continue

            # 计算实际采集的单日列表（配置 & 活动有效期）
            actual_start = max(config_start, camp_start)
            actual_end = min(config_end, camp_end)
            single_dates = get_date_range(actual_start, actual_end)

            print(f"\n📌 活动{camp_id}：待采集单日：{single_dates}")

            # 遍历地域维度 + 单日采集
            for by_region in CONFIG["report_params"]["by_region_list"]:
                for single_date in single_dates:
                    # 采集单条数据
                    row = collect_report_data(token, camp_id, single_date, by_region)
                    if not row:
                        continue

                    # 按单日分区分组
                    partition_dt = get_partition_dt(single_date)
                    if partition_dt not in date_data_map:
                        date_data_map[partition_dt] = []
                    date_data_map[partition_dt].append(row)

                    time.sleep(CONFIG["api"]["interval"])  # 接口限流间隔

        # 7. 写入MaxCompute（按单日分区批量写入）
        if date_data_map:
            print("\n" + "=" * 60)
            print("📤 开始写入MaxCompute（按单日分区）")
            print("=" * 60)
            for partition_dt, data in date_data_map.items():
                write_data_to_odps(odps, data, partition_dt)
        else:
            print("\n⚠️ 无有效数据可写入MaxCompute")

        # 8. 任务完成统计
        total_rows = sum(len(data) for data in date_data_map.values())
        print("\n" + "=" * 80)
        print("✅ 秒针日报表采集+入库任务完成！")
        print(f"📊 ODPS项目：{ODPS_PROJECT}")
        print(f"📊 总写入分区数：{len(date_data_map)}个")
        print(f"📊 总写入数据条数：{total_rows}条")
        print(f"📊 覆盖分区：{list(date_data_map.keys())}")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()