# -*- coding: utf-8 -*-
import requests
import json
import os
from datetime import datetime, timedelta
import time
import argparse
import urllib3
from typing import Dict, List, Optional, Any
from odps import ODPS, options, TableSchema, Partition  # MaxCompute SDK
from odps.models import Column

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

# 1. 本地保存配置（DataWorks中可注释，仅用于本地调试）
# LOCAL_SAVE_DIR = "./miaozhen_data"
# FILE_ENCODING = "utf-8"

# 2. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "report_basic_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/reports/basic/show",
    "auth": {
        "username": "Coach_api",
        "password": "Coachapi2026"
    },
    "timeout": 30,
    "request_interval": 0.2,
    "debug": False  # DataWorks中建议关闭debug，减少日志输出
}

# 3. MaxCompute配置（DataWorks自动鉴权版）
# 仅需配置项目名，无需access_id/access_key
MAXCOMPUTE_CONFIG = {
    "project": "your_maxcompute_project_name",  # 替换为你的MaxCompute项目名（与DataWorks一致）
    "endpoint": "http://service.maxcompute.aliyun-inc.com/api"  # DataWorks内网Endpoint
}

# 4. 表名配置
TABLE_NAMES = {
    "campaign": "ods_mz_tvm_cms_campaigns_list_api_df",
    "report": "ods_mz_tvm_admonitor_basic_show_api_di"
}


# ===================== MaxCompute初始化（DataWorks自动鉴权） =====================
def init_odps_client() -> ODPS:
    """
    初始化MaxCompute客户端（DataWorks内置环境自动鉴权）
    无需access_id/access_key，自动读取DataWorks容器内的鉴权信息
    """
    try:
        # DataWorks自动鉴权：仅需传入project和endpoint，无需AK
        odps = ODPS(
            project=MAXCOMPUTE_CONFIG["project"],
            endpoint=MAXCOMPUTE_CONFIG["endpoint"]
        )

        # DataWorks适配配置
        options.tunnel.limit_instance_tunnel = False  # 关闭实例隧道限制
        options.default_retry_times = 3  # 重试次数
        options.charset = 'utf-8'  # 字符集
        options.use_instance_tunnel = True  # 使用实例隧道（DataWorks推荐）

        print("✅ MaxCompute客户端初始化成功（DataWorks自动鉴权）")
        return odps
    except Exception as e:
        raise Exception(f"❌ MaxCompute客户端初始化失败：{str(e)}")


# ===================== 命令行参数解析（DataWorks调度适配） =====================
def parse_args():
    """
    解析采集日期参数
    DataWorks调度时可通过传参指定dt，或默认使用前一天日期
    """
    parser = argparse.ArgumentParser(description='秒针数据采集并写入MaxCompute（DataWorks版）')
    parser.add_argument('--dt', type=str, default=None, help='采集日期（格式：yyyyMMdd），默认前一天')
    args = parser.parse_args()

    # 处理默认日期（DataWorks调度常用：默认采集前一天数据）
    if not args.dt:
        yesterday = datetime.now() - timedelta(days=1)
        args.dt = yesterday.strftime("%Y%m%d")
    else:
        # 日期格式校验
        try:
            datetime.strptime(args.dt, "%Y%m%d")
        except ValueError:
            raise ValueError(f"❌ 日期格式错误！dt={args.dt}，请使用yyyyMMdd格式")

    return args


# ===================== 通用工具函数 =====================
def get_etl_time() -> str:
    """获取当前时间戳（格式：yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_date_str(dt: str) -> str:
    """将yyyyMMdd转换为yyyy-MM-dd格式"""
    return datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")


def safe_convert_type(value, target_type: str) -> Any:
    """
    安全转换数据类型（兼容空值）
    :param value: 原始值
    :param target_type: 目标类型（bigint/int/string）
    :return: 转换后的值，失败返回None/空字符串
    """
    if value is None or value == "" or value == "null":
        if target_type in ("bigint", "int"):
            return None
        else:
            return ""

    try:
        if target_type == "bigint":
            return int(value)
        elif target_type == "int":
            return int(value)
        elif target_type == "string":
            return str(value)
        else:
            return str(value)
    except (ValueError, TypeError):
        if target_type in ("bigint", "int"):
            return None
        else:
            return str(value) if value is not None else ""


# ===================== 秒针接口调用函数 =====================
def get_miaozhen_token() -> str:
    """获取秒针Token"""
    print("\n🔍 请求秒针Token...")
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
        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误 | 码：{token_data.get('error_code')} | 信息：{token_data.get('error_message')}")

        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token为空！")

        print("✅ Token获取成功")
        return access_token
    except Exception as e:
        raise Exception(f"❌ Token获取失败：{str(e)}")


def get_campaign_list(token: str) -> List[Dict]:
    """
    获取活动列表数据（适配MaxCompute表结构）
    """
    print("\n🔍 采集活动列表数据...")
    try:
        request_url = f"{API_CONFIG['campaign_list_url']}?access_token={token}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        response = requests.get(
            url=request_url,
            headers=headers,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        # 获取原始文本
        api_raw_text = response.text
        raw_response = json.loads(api_raw_text) if api_raw_text else {}

        # 提取campaigns数组
        campaigns = raw_response.get("result", {}).get("campaigns", [])
        if not isinstance(campaigns, list):
            campaigns = []

        # 适配MaxCompute表结构
        campaign_list = []
        etl_time = get_etl_time()
        for campaign in campaigns:
            if not isinstance(campaign, dict):
                continue

            # 单条campaign原始文本
            single_raw_text = json.dumps(campaign, ensure_ascii=False)

            # 按表结构映射字段 + 类型转换
            campaign_row = {
                "campaign_id": safe_convert_type(campaign.get("campaign_id"), "bigint"),
                "start_time": safe_convert_type(campaign.get("start_time"), "string"),
                "end_time": safe_convert_type(campaign.get("end_time"), "string"),
                "order_id": safe_convert_type(campaign.get("order_id"), "string"),
                "scheduling": safe_convert_type(campaign.get("scheduling"), "string"),
                "campaign_name": safe_convert_type(campaign.get("campaign_name"), "string"),
                "description": safe_convert_type(campaign.get("description"), "string"),
                "created_time": safe_convert_type(campaign.get("created_time"), "string"),
                "advertiser": safe_convert_type(campaign.get("advertiser"), "string"),
                "agency": safe_convert_type(campaign.get("agency"), "string"),
                "brand": safe_convert_type(campaign.get("brand"), "string"),
                "status": safe_convert_type(campaign.get("status"), "string"),
                "verify_version": safe_convert_type(campaign.get("verify_version"), "int"),
                "total_net_id": safe_convert_type(campaign.get("total_net_id"), "string"),
                "calculate_type": safe_convert_type(campaign.get("calculate_type"), "string"),
                "totalnet_version": safe_convert_type(campaign.get("totalnet_version"), "string"),
                "sivt_region": safe_convert_type(campaign.get("sivt_region"), "string"),
                "target_list": safe_convert_type(campaign.get("target_list"), "string"),  # JSON格式
                "order_title": safe_convert_type(campaign.get("order_title"), "string"),
                "pre_parse_raw_text": single_raw_text,
                "etl_time": etl_time
            }
            campaign_list.append(campaign_row)

        print(f"✅ 活动列表解析完成 | 有效条数：{len(campaign_list)}")
        return campaign_list
    except Exception as e:
        raise Exception(f"❌ 活动列表采集失败：{str(e)}")


def get_daily_report_data(token: str, campaign_id: str, report_date: str) -> Optional[Dict]:
    """
    获取日报数据（适配MaxCompute表结构）
    """
    try:
        request_url = f"{API_CONFIG['report_basic_url']}?access_token={token}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        params = {"campaign_id": campaign_id, "date": report_date}

        response = requests.get(
            url=request_url,
            headers=headers,
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        response.raise_for_status()

        # 获取原始文本
        api_raw_text = response.text
        raw_report = json.loads(api_raw_text) if api_raw_text else {}

        # 按表结构映射字段
        report_row = {
            "campaign_id": safe_convert_type(campaign_id, "string"),
            "start_date": safe_convert_type(raw_report.get("start_date"), "string"),
            "end_date": safe_convert_type(raw_report.get("end_date"), "string"),
            "date": safe_convert_type(report_date, "string"),
            "version": safe_convert_type(raw_report.get("version"), "string"),
            "platform": safe_convert_type(raw_report.get("platform"), "string"),
            "total_spot_num": safe_convert_type(raw_report.get("total_spot_num"), "string"),
            "audience": safe_convert_type(raw_report.get("audience"), "string"),
            "target_id": safe_convert_type(raw_report.get("target_id"), "string"),
            "publisher_id": safe_convert_type(raw_report.get("publisher_id"), "string"),
            "spot_id": safe_convert_type(raw_report.get("spot_id"), "string"),
            "keyword_id": safe_convert_type(raw_report.get("keyword_id"), "string"),
            "region_id": safe_convert_type(raw_report.get("region_id"), "string"),
            "universe": safe_convert_type(raw_report.get("universe"), "string"),
            "imp_acc": safe_convert_type(raw_report.get("imp_acc"), "string"),
            "clk_acc": safe_convert_type(raw_report.get("clk_acc"), "string"),
            "uim_acc": safe_convert_type(raw_report.get("uim_acc"), "string"),
            "ucl_acc": safe_convert_type(raw_report.get("ucl_acc"), "string"),
            "imp_day": safe_convert_type(raw_report.get("imp_day"), "string"),
            "clk_day": safe_convert_type(raw_report.get("clk_day"), "string"),
            "uim_day": safe_convert_type(raw_report.get("uim_day"), "string"),
            "ucl_day": safe_convert_type(raw_report.get("ucl_day"), "string"),
            "imp_avg_day": safe_convert_type(raw_report.get("imp_avg_day"), "string"),
            "clk_avg_day": safe_convert_type(raw_report.get("clk_avg_day"), "string"),
            "uim_avg_day": safe_convert_type(raw_report.get("uim_avg_day"), "string"),
            "ucl_avg_day": safe_convert_type(raw_report.get("ucl_avg_day"), "string"),
            "imp_acc_h00": safe_convert_type(raw_report.get("imp_acc_h00"), "string"),
            "imp_acc_h23": safe_convert_type(raw_report.get("imp_acc_h23"), "string"),
            "clk_acc_h00": safe_convert_type(raw_report.get("clk_acc_h00"), "string"),
            "clk_acc_h23": safe_convert_type(raw_report.get("clk_acc_h23"), "string"),
            "pre_parse_raw_text": api_raw_text,
            "etl_date": get_etl_time().split(" ")[0]  # 格式：yyyy-MM-dd
        }

        if API_CONFIG["debug"]:
            print(f"  📝 日报数据 | 活动{campaign_id} | 日期{report_date} | 解析完成")
        return report_row

    except Exception as e:
        print(f"  ❌ 活动{campaign_id} {report_date}日报采集失败：{str(e)}")
        return None


# ===================== MaxCompute数据写入函数（DataWorks适配） =====================
def write_campaign_to_maxcompute(odps_client: ODPS, campaign_data: List[Dict], dt: str):
    """
    写入活动列表数据到MaxCompute（DataWorks适配）
    """
    if not campaign_data:
        print(f"⚠️ 无活动数据可写入MaxCompute | 分区：{dt}")
        return

    try:
        table_name = TABLE_NAMES["campaign"]
        table = odps_client.get_table(table_name)

        # 准备写入数据（按表字段顺序）
        write_rows = []
        for row in campaign_data:
            write_row = [
                row.get("campaign_id"),
                row.get("start_time"),
                row.get("end_time"),
                row.get("order_id"),
                row.get("scheduling"),
                row.get("campaign_name"),
                row.get("description"),
                row.get("created_time"),
                row.get("advertiser"),
                row.get("agency"),
                row.get("brand"),
                row.get("status"),
                row.get("verify_version"),
                row.get("total_net_id"),
                row.get("calculate_type"),
                row.get("totalnet_version"),
                row.get("sivt_region"),
                row.get("target_list"),
                row.get("order_title"),
                row.get("pre_parse_raw_text"),
                row.get("etl_time"),
                dt  # 分区字段
            ]
            write_rows.append(write_row)

        # DataWorks推荐：使用instance tunnel批量写入
        with table.open_writer(
                partition=Partition(table, (dt,)),
                create_partition=True,
                tunnel_type='instance'  # 指定instance tunnel
        ) as writer:
            writer.write(write_rows)

        print(f"✅ 活动列表写入MaxCompute成功 | 表：{table_name} | 分区：{dt} | 条数：{len(write_rows)}")
    except Exception as e:
        raise Exception(f"❌ 活动列表写入MaxCompute失败 | 分区：{dt} | 错误：{str(e)}")


def write_report_to_maxcompute(odps_client: ODPS, report_data: List[Dict], dt: str):
    """
    写入日报数据到MaxCompute（DataWorks适配）
    """
    if not report_data:
        print(f"⚠️ 无日报数据可写入MaxCompute | 分区：{dt}")
        return

    try:
        table_name = TABLE_NAMES["report"]
        table = odps_client.get_table(table_name)

        # 准备写入数据（按表字段顺序）
        write_rows = []
        for row in report_data:
            write_row = [
                row.get("campaign_id"),
                row.get("start_date"),
                row.get("end_date"),
                row.get("date"),
                row.get("version"),
                row.get("platform"),
                row.get("total_spot_num"),
                row.get("audience"),
                row.get("target_id"),
                row.get("publisher_id"),
                row.get("spot_id"),
                row.get("keyword_id"),
                row.get("region_id"),
                row.get("universe"),
                row.get("imp_acc"),
                row.get("clk_acc"),
                row.get("uim_acc"),
                row.get("ucl_acc"),
                row.get("imp_day"),
                row.get("clk_day"),
                row.get("uim_day"),
                row.get("ucl_day"),
                row.get("imp_avg_day"),
                row.get("clk_avg_day"),
                row.get("uim_avg_day"),
                row.get("ucl_avg_day"),
                row.get("imp_acc_h00"),
                row.get("imp_acc_h23"),
                row.get("clk_acc_h00"),
                row.get("clk_acc_h23"),
                row.get("pre_parse_raw_text"),
                row.get("etl_date"),
                dt  # 分区字段
            ]
            write_rows.append(write_row)

        # DataWorks推荐：使用instance tunnel批量写入
        with table.open_writer(
                partition=Partition(table, (dt,)),
                create_partition=True,
                tunnel_type='instance'  # 指定instance tunnel
        ) as writer:
            writer.write(write_rows)

        print(f"✅ 日报数据写入MaxCompute成功 | 表：{table_name} | 分区：{dt} | 条数：{len(write_rows)}")
    except Exception as e:
        raise Exception(f"❌ 日报数据写入MaxCompute失败 | 分区：{dt} | 错误：{str(e)}")


# ===================== 主函数（DataWorks调度适配） =====================
def main():
    """核心逻辑：采集数据 → 格式适配 → 写入MaxCompute（DataWorks版）"""
    try:
        # 1. 解析参数（DataWorks调度传dt，默认前一天）
        args = parse_args()
        partition_dt = args.dt  # 分区日期（yyyyMMdd）
        report_date = get_date_str(partition_dt)  # 转换为yyyy-MM-dd格式

        # 打印任务信息
        print("=" * 80)
        print("秒针数据采集并写入MaxCompute（DataWorks自动鉴权版）")
        print(f"执行时间：{get_etl_time()}")
        print(f"采集日期：{partition_dt}（{report_date}）")
        print(f"目标表1：{TABLE_NAMES['campaign']}")
        print(f"目标表2：{TABLE_NAMES['report']}")
        print("=" * 80)

        # 2. 初始化MaxCompute客户端（DataWorks自动鉴权）
        odps_client = init_odps_client()

        # 3. 获取秒针Token
        token = get_miaozhen_token()

        # ========== 步骤1：采集并写入活动列表 ==========
        print(f"\n🔹 步骤1：采集活动列表并写入MaxCompute")
        campaign_data = get_campaign_list(token)
        if campaign_data:
            write_campaign_to_maxcompute(odps_client, campaign_data, partition_dt)
        else:
            print(f"⚠️ {partition_dt} 无有效活动数据")

        # ========== 步骤2：采集并写入日报数据 ==========
        print(f"\n🔹 步骤2：采集日报数据并写入MaxCompute")
        report_data_list = []
        # 遍历活动列表采集日报
        for campaign in campaign_data:
            camp_id = campaign.get("campaign_id")
            if not camp_id:
                continue

            report_row = get_daily_report_data(token, str(camp_id), report_date)
            if report_row:
                report_data_list.append(report_row)

            time.sleep(API_CONFIG["request_interval"])

        # 写入日报数据
        if report_data_list:
            write_report_to_maxcompute(odps_client, report_data_list, partition_dt)
            print(f"✅ {partition_dt} 日报数据采集完成 | 条数：{len(report_data_list)}")
        else:
            print(f"⚠️ {partition_dt} 无有效日报数据")

        # 任务总结
        print("\n" + "=" * 80)
        print("✅ 所有数据写入MaxCompute完成！")
        print(f"📊 最终统计：")
        print(f"   - 采集日期：{partition_dt}")
        print(f"   - 活动列表：{len(campaign_data)} 条（写入 {TABLE_NAMES['campaign']}）")
        print(f"   - 日报数据：{len(report_data_list)} 条（写入 {TABLE_NAMES['report']}）")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        # DataWorks中抛出异常，触发任务失败告警
        raise e


# ===================== DataWorks部署说明 =====================
"""
### 1. DataWorks脚本部署步骤
1. 登录DataWorks控制台 → 进入对应项目 → 数据开发 → 新建Python节点
2. 将本代码复制到Python节点中
3. 修改配置：
   - MAXCOMPUTE_CONFIG["project"]：替换为你的MaxCompute项目名
   - API_CONFIG["auth"]：确认秒针接口的账号密码正确
4. 配置调度（可选）：
   - 调度周期：按天调度
   - 运行参数：--dt ${bdp.system.bizdate}（使用DataWorks业务日期）

### 2. 依赖说明
DataWorks内置环境已预装odps SDK，无需额外安装

### 3. 权限说明
确保DataWorks项目对应的RAM角色具备：
- MaxCompute表的写入权限（INSERT）
- MaxCompute分区的创建权限（ALTER）
"""

if __name__ == "__main__":
    main()