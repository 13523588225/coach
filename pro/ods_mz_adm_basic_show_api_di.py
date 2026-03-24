# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置（MaxCompute + 日期参数） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 自动获取ODPS项目名
ODPS_PROJECT = ODPS().project  # 从odps配置文件自动读取默认项目名

CONFIG = {
    "start_dt": "20260309",  # 配置开始日期（yyyyMMdd）
    "end_dt": "20260311",  # 配置结束日期（yyyyMMdd）
    # MaxCompute配置（适配新表名）
    "odps": {
        "project": ODPS_PROJECT,
        "table_name": "coach_marketing_hub_dev.ods_mz_adm_basic_show_api_di",  # 目标表名
        "partition_col": "dt",  # 分区字段（yyyyMMdd）
        "batch_size": 1000  # 批量写入大小
    },
    # 报表参数：适配新表维度字段
    "report_params": {
        "metrics": "all",
        "by_region_list": ["level0", "level1", "level2"],
        "by_position": "spot"  # 固定值：spot
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
    """安全转换字符串，空值/特殊值返回None"""
    if val is None or val == "" or val == "-" or val in ("null", "undefined"):
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
    写入数据到MaxCompute指定分区（适配新表结构）
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
    """
    获取有效活动列表（含ID+活动起止日期，适配新表字段）
    返回：包含campaign_id/campaign_start_date/campaign_end_date的字典列表
    """
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

            # 提取活动核心字段（适配新表）
            camp_id = camp.get("campaign_id")
            campaign_start_date = camp.get("start_date")  # 活动开始日期
            campaign_end_date = camp.get("end_date")  # 活动结束日期

            # 校验字段完整性
            if not camp_id or not campaign_start_date or not campaign_end_date:
                print(f"⚠️ 第{idx + 1}条活动数据核心字段缺失，跳过：{camp.get('campaign_id', '未知ID')}")
                continue

            # 校验日期格式
            try:
                datetime.strptime(campaign_start_date, "%Y-%m-%d")
                datetime.strptime(campaign_end_date, "%Y-%m-%d")
            except Exception as e:
                print(f"⚠️ 活动{camp_id}日期格式错误，跳过：{str(e)}")
                continue

            valid_camps.append({
                "campaign_id": str(camp_id).strip(),
                "campaign_start_date": campaign_start_date,
                "campaign_end_date": campaign_end_date
            })

        print(f"✅ 获取到{len(valid_camps)}个有效活动（含有效期）")
        for camp in valid_camps:
            print(f"   - 活动{camp['campaign_id']} | 有效期：{camp['campaign_start_date']}~{camp['campaign_end_date']}")
        return valid_camps
    except Exception as e:
        raise Exception(f"活动列表获取失败：{str(e)}")


def collect_report_data(token, campaign_info, single_date, by_region):
    """
    采集单活动+单日期+单地域维度的报表数据（适配真实接口返回结构）
    :param campaign_info: 活动信息字典（含campaign_id/campaign_start_date/campaign_end_date）
    :param single_date: 报表采集日期（yyyy-MM-dd）
    :param by_region: 地域维度值
    :return: 与新表字段一一对应的行数据列表（多条，对应items中的每个元素）
    """
    rows = []  # 存储当前采集的所有行数据（items列表中的每个元素对应一行）
    try:
        campaign_id = campaign_info["campaign_id"]
        print(f"  🔍 采集活动{campaign_id} | 地域[{by_region}] | 日期[{single_date}]...")

        # 新接口参数（date替代start/end_date）
        params = {
            "access_token": token,
            "campaign_id": campaign_id,
            "date": single_date,
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

        # 提取接口顶层字段
        report_date = parsed_data.get("date")  # 报表日期
        total_spot_num = parsed_data.get("total_spot_num")  # 总点位数量
        version = parsed_data.get("version")  # 版本号
        platform = parsed_data.get("platform")  # 平台类型
        raw_text = resp.text  # 原始返回文本

        # 遍历items列表（每个item对应一行数据）
        items = parsed_data.get("items", [])
        if not items:
            print(f"  ⚠️ 活动{campaign_id}日期[{single_date}]无items数据，跳过")
            return rows

        for item in items:
            # 提取attributes中的字段
            attrs = item.get("attributes", {})
            region_id = attrs.get("region_id")
            publisher_id = attrs.get("publisher_id")
            audience = attrs.get("audience")
            spot_id = attrs.get("spot_id")
            universe = attrs.get("universe")

            # 提取metrics中的字段
            metrics = item.get("metrics", {})
            imp_acc = metrics.get("imp_acc")
            clk_acc = metrics.get("clk_acc")
            uim_acc = metrics.get("uim_acc")
            ucl_acc = metrics.get("ucl_acc")
            imp_day = metrics.get("imp_day")
            clk_day = metrics.get("clk_day")
            uim_day = metrics.get("uim_day")
            ucl_day = metrics.get("ucl_day")
            imp_avg_day = metrics.get("imp_avg_day")
            clk_avg_day = metrics.get("clk_avg_day")
            uim_avg_day = metrics.get("uim_avg_day")
            ucl_avg_day = metrics.get("ucl_avg_day")
            imp_acc_h00 = metrics.get("imp_h00")  # 累计曝光量（0点）
            imp_acc_h23 = metrics.get("imp_h23")  # 累计曝光量（23点）
            clk_acc_h00 = metrics.get("clk_h00")  # 累计点击量（0点）
            clk_acc_h23 = metrics.get("clk_h23")  # 累计点击量（23点）

            # 组装单条数据（与新表字段完全一一对应）
            row = [
                # 1. 活动核心信息字段
                safe_str(campaign_id),  # campaign_id
                safe_str(campaign_info["campaign_start_date"]),  # campaign_start_date
                safe_str(campaign_info["campaign_end_date"]),  # campaign_end_date

                # 2. 报表采集日期字段
                safe_str(report_date),  # report_day_date

                # 3. 报表维度参数字段
                safe_str(CONFIG["report_params"]["by_position"]),  # by_position（固定spot）
                safe_str(by_region),  # by_region
                safe_str(CONFIG["report_params"]["metrics"]),  # metrics（固定all）

                # 4. 秒针返回的核心指标字段
                safe_str(version),  # version
                safe_str(platform),  # platform
                safe_str(total_spot_num),  # total_spot_num
                safe_str(audience),  # audience
                None,  # target_id（接口无该字段，填None）
                safe_str(publisher_id),  # publisher_id
                safe_str(spot_id),  # spot_id
                None,  # keyword_id（接口无该字段，填None）
                safe_str(region_id),  # region_id
                safe_str(universe),  # universe
                safe_str(imp_acc),  # imp_acc
                safe_str(clk_acc),  # clk_acc
                safe_str(uim_acc),  # uim_acc
                safe_str(ucl_acc),  # ucl_acc
                safe_str(imp_day),  # imp_day
                safe_str(clk_day),  # clk_day
                safe_str(uim_day),  # uim_day
                safe_str(ucl_day),  # ucl_day
                safe_str(imp_avg_day),  # imp_avg_day
                safe_str(clk_avg_day),  # clk_avg_day
                safe_str(uim_avg_day),  # uim_avg_day
                safe_str(ucl_avg_day),  # ucl_avg_day
                safe_str(imp_acc_h00),  # imp_acc_h00
                safe_str(imp_acc_h23),  # imp_acc_h23
                safe_str(clk_acc_h00),  # clk_acc_h00
                safe_str(clk_acc_h23),  # clk_acc_h23

                # 5. 原始数据与采集元信息
                safe_str(raw_text),  # pre_parse_raw_text
                get_etl_datetime()  # etl_datetime
            ]
            rows.append(row)

        print(f"  ✅ 活动{campaign_id}日期[{single_date}]采集成功 | 共{len(rows)}条数据")
        return rows
    except Exception as e:
        print(f"  ❌ 活动{campaign_info['campaign_id']}日期[{single_date}]采集失败：{str(e)}")
        return rows


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针日报表采集任务启动（写入MaxCompute + 适配真实接口结构）")
    print(f"📅 配置日期区间：{CONFIG['start_dt']} ~ {CONFIG['end_dt']}")
    print(f"🔧 ODPS项目：{ODPS_PROJECT}")
    print(f"🔧 目标表：{CONFIG['odps']['table_name']}")
    print(
        f"🔧 报表参数：metrics={CONFIG['report_params']['metrics']} | by_position={CONFIG['report_params']['by_position']}")
    print(f"🔧 地域维度：{CONFIG['report_params']['by_region_list']}")
    print("=" * 80)

    try:
        # 1. 校验ODPS项目名是否获取成功
        if not ODPS_PROJECT:
            raise Exception("ODPS项目名自动获取失败，请检查~/.odps_config.ini配置")

        # 2. 初始化ODPS客户端
        odps = init_odps_client()

        # 3. 获取秒针Token
        token = get_miaozhen_token()

        # 4. 转换配置日期格式
        config_start = format_date(CONFIG["start_dt"])
        config_end = format_date(CONFIG["end_dt"])

        # 5. 获取有效活动列表（含活动起止日期）
        valid_camps = get_valid_campaigns(token)
        if not valid_camps:
            print("⚠️ 未获取到任何有效活动，任务终止")
            return

        # 6. 按单日分区存储数据（核心逻辑）
        date_data_map = {}  # key: 分区日期(yyyyMMdd), value: 待写入数据列表

        for camp in valid_camps:
            camp_id = camp["campaign_id"]
            camp_start = camp["campaign_start_date"]
            camp_end = camp["campaign_end_date"]

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
                    # 采集数据（返回多条行数据）
                    item_rows = collect_report_data(token, camp, single_date, by_region)
                    if not item_rows:
                        continue

                    # 按单日分区分组
                    partition_dt = get_partition_dt(single_date)
                    if partition_dt not in date_data_map:
                        date_data_map[partition_dt] = []
                    date_data_map[partition_dt].extend(item_rows)  # 追加多条数据

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
        print(f"📊 目标表：{CONFIG['odps']['table_name']}")
        print(f"📊 总写入分区数：{len(date_data_map)}个")
        print(f"📊 总写入数据条数：{total_rows}条")
        print(f"📊 覆盖分区：{list(date_data_map.keys())}")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()