"""
秒针Campaign广告位详情采集脚本
功能：采集数据并分批写入ODPS（仅首次清空分区，最终存储全量数据）
新增：1. 任务执行时间统计 2. campaign_show_spot_url接口并行访问（并行度=10）
数据来源：/cms/v1/campaigns/show_spot 接口
适配表结构：ods_mz_adm_show_spot_api_df（含keyword字段）
依赖：odps库（pip install odps）
"""
import requests
import json
import time
import urllib3
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from odps import ODPS, errors
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== 全局配置 & 初始化 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 全局ODPS项目变量（自动获取）
ODPS_PROJECT = ODPS().project

# 核心配置
CONFIG = {
    # MaxCompute配置（字段顺序完全匹配指定表结构）
    "odps": {
        "table_name": "ods_mz_adm_show_spot_api_df",
        "partition_col": "dt",
        "table_columns": [
            "request_campaign_id",
            "request_spot_id_str",
            "publisher_id",
            "channel_name",
            "publisher_name",
            "spot_id",
            "GUID",
            "description",
            "CAGUID",
            "customize",
            "report_metrics",
            "market",
            "vending_model",
            "area_size",
            "linked_siteid",
            "placement_name",
            "spot_id_str",
            "keyword",  # 关键词列表（JSON格式）
            "adposition_type",
            "full_request_url",  # 新增：完整接口请求URL
            "pre_parse_raw_text",
            "etl_datetime"
        ]
    },
    # API配置
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "auth": {
            "grant_type": "password",
            "username": "",  # 动态从数仓获取
            "password": "",  # 动态从数仓获取
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "campaign_list_spots_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_spots",
        "campaign_show_spot_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/show_spot",
        "timeout": 30,
        "interval": 0.02,
        "parallelism": 10  # campaign_show_spot_url接口并行度
    },
    "batch_size": 1000  # 批量写入大小（1000条/批）
}

# 全局标记：是否已清空分区（确保仅清空一次）
PARTITION_CLEARED = False


# ===================== 日志工具函数 =====================
def get_log() -> str:
    """获取格式化日志时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 从MaxCompute查询API账号密码 =====================
def get_adm_api_credentials():
    """
    从数仓表 ods_mz_user_api_df 查询ADM接口的账号密码
    """
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'ADM' limit 1 \
          """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_log()}] 🔐 成功从数仓获取ADM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"❌ 查询账号密码失败：{str(e)}")


# ====================== 通用ODPS写入函数（核心优化：仅首次清空分区） ======================
def write_to_odps(table_name: str, data: List[List], dt: str):
    """
    通用ODPS写入函数
    核心逻辑：仅第一次写入前清空分区，后续批次直接追加，保证全量数据存储
    """
    global PARTITION_CLEARED
    if not data:
        print(f"⚠️ {table_name} 无数据可写入")
        return

    # 初始化ODPS客户端
    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"❌ 表{table_name}不存在，请检查表名或ODPS配置")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    # 仅第一次写入前清空分区（防止重复，后续批次直接追加）
    if not PARTITION_CLEARED and table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 首次清空分区：{table_name}.{partition_spec}")
        PARTITION_CLEARED = True  # 标记为已清空，后续不再执行

    # 写入数据（追加模式）
    try:
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            writer.write(data)
        print(f"✅ 成功写入{len(data)}条数据到分区{partition_spec}")
    except Exception as e:
        raise Exception(f"❌ 数据写入失败：{str(e)}")


# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """
    强制转换为字符串，处理空值/None/-
    特殊处理：列表/字典序列化为JSON字符串（适配keyword字段）
    """
    if value is None or value == "" or str(value).lower() == "null" or value == "-":
        return ""
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except:
            return ""
    return str(value)


# ===================== Access Token获取 =====================
def get_access_token() -> str:
    """获取OAuth Access Token（异常捕获+校验）"""
    try:
        # 从数仓获取账号密码
        username, password = get_adm_api_credentials()
        CONFIG["api"]["auth"]["username"] = username
        CONFIG["api"]["auth"]["password"] = password

        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=CONFIG["api"]["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()  # 触发HTTP错误异常
        token_data = resp.json()

        access_token = token_data.get("access_token")
        if not access_token:
            raise Exception(f"Token返回无access_token字段，返回数据：{token_data}")

        print(f"✅ Access Token获取成功（前10位：{access_token[:10]}...）")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"❌ 获取Access Token失败：{str(e)}")


# ===================== 获取所有campaign_id =====================
def get_campaign_ids(token: str) -> List[str]:
    """从/cms/v1/campaigns/list接口获取所有campaign_id（兼容多格式返回）"""
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()

        # 解析campaign_id（兼容列表/字典格式返回）
        campaign_ids = []
        if isinstance(raw_data, list):
            campaign_ids = [to_string(item.get("campaign_id")) for item in raw_data if item.get("campaign_id")]
        elif isinstance(raw_data, dict):
            data_nodes = ["result", "list", "data", "campaigns"]
            for node in data_nodes:
                if node in raw_data and isinstance(raw_data[node], list):
                    campaign_ids = [to_string(item.get("campaign_id")) for item in raw_data[node] if
                                    item.get("campaign_id")]
                    break
            if not campaign_ids and raw_data.get("campaign_id"):
                campaign_ids = [to_string(raw_data.get("campaign_id"))]

        # 去重+过滤空值
        campaign_ids = list(set([cid for cid in campaign_ids if cid]))
        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        time.sleep(CONFIG["api"]["interval"])
        return campaign_ids
    except Exception as e:
        raise Exception(f"❌ 获取campaign_id失败：{str(e)}")


# ===================== 获取单个campaign的spot_id_str列表 =====================
def get_spot_id_str_list(token: str, campaign_id: str) -> List[str]:
    """从/cms/v1/campaigns/list_spots接口获取单个campaign的spot_id_str列表"""
    try:
        params = {
            "campaign_id": campaign_id,
            "access_token": token
        }
        resp = requests.get(
            CONFIG["api"]["campaign_list_spots_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spots_data = resp.json()

        # 解析spot_id_str
        spot_id_str_list = []
        if isinstance(spots_data, list):
            spot_id_str_list = [to_string(item.get("spot_id_str")) for item in spots_data if item.get("spot_id_str")]
        elif isinstance(spots_data, dict) and "result" in spots_data:
            spot_id_str_list = [to_string(item.get("spot_id_str")) for item in spots_data["result"] if
                                item.get("spot_id_str")]

        # 去重+过滤空值
        spot_id_str_list = list(set([sid for sid in spot_id_str_list if sid]))
        print(f"✅ campaign_id={campaign_id} 获取到{len(spot_id_str_list)}个有效spot_id_str")
        time.sleep(CONFIG["api"]["interval"])
        return spot_id_str_list
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id} 获取spot_id_str失败，跳过：{str(e)}")
        return []


# ===================== 采集单个广告位详情（适配keyword字段） =====================
def get_spot_detail_worker(token: str, campaign_id: str, spot_id_str: str) -> Optional[Dict]:
    """
    单个广告位详情采集工作函数（供线程池调用）
    核心：处理keyword字段（JSON格式）、接口必传参数keyword=on
    """
    try:
        # 接口请求参数（新增必传参数keyword=on）
        params = {
            "campaign_id": campaign_id,
            "spot_id_str": spot_id_str,
            "access_token": token,
            "keyword": "on"
        }
        # 拼接完整请求URL
        full_url = requests.Request('GET', CONFIG["api"]["campaign_show_spot_url"], params=params).prepare().url

        resp = requests.get(
            CONFIG["api"]["campaign_show_spot_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_detail = resp.json()

        if not isinstance(spot_detail, dict):
            print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 返回非字典格式，跳过")
            return None

        # 标准化字段（严格匹配表字段顺序）
        etl_datetime = get_etl_datetime()
        standard_spot = {}
        for col in CONFIG["odps"]["table_columns"]:
            if col == "request_campaign_id":
                standard_spot[col] = to_string(campaign_id)
            elif col == "request_spot_id_str":
                standard_spot[col] = to_string(spot_id_str)
            elif col == "etl_datetime":
                standard_spot[col] = etl_datetime
            elif col == "keyword":
                # 特殊处理：keyword字段序列化为JSON字符串
                standard_spot[col] = to_string(spot_detail.get(col, ""))
            elif col == "full_request_url":
                # 新增字段：完整接口请求URL
                standard_spot[col] = to_string(full_url)
            else:
                # 通用字段处理
                standard_spot[col] = to_string(spot_detail.get(col, ""))

        # 补充原始文本字段（完整接口返回数据，用于排查问题）
        standard_spot["pre_parse_raw_text"] = to_string(json.dumps(spot_detail, ensure_ascii=False))

        print(
            f"✅ campaign_id={campaign_id}, spot_id_str={spot_id_str} 采集成功（线程：{threading.current_thread().name}）")
        time.sleep(CONFIG["api"]["interval"])
        return standard_spot
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 采集失败，跳过：{str(e)}")
        return None


# ===================== 批量并行采集广告位详情 =====================
def batch_get_spot_detail(token: str, campaign_id: str, spot_id_str_list: List[str]) -> List[Dict]:
    """
    批量并行采集广告位详情
    核心：使用线程池实现10线程并行调用campaign_show_spot_url接口
    """
    spot_detail_list = []
    parallelism = CONFIG["api"]["parallelism"]

    # 创建线程池，并行度=10
    with ThreadPoolExecutor(max_workers=parallelism, thread_name_prefix="SpotDetail") as executor:
        # 提交所有采集任务
        future_to_spot = {
            executor.submit(get_spot_detail_worker, token, campaign_id, spot_id_str): spot_id_str
            for spot_id_str in spot_id_str_list
        }

        # 处理完成的任务
        for future in as_completed(future_to_spot):
            spot_id_str = future_to_spot[future]
            try:
                result = future.result()
                if result:
                    spot_detail_list.append(result)
            except Exception as e:
                print(f"⚠️ campaign_id={campaign_id}, spot_id_str={spot_id_str} 线程执行异常，跳过：{str(e)}")

    return spot_detail_list


# ===================== 数据转换（适配ODPS写入格式） =====================
def convert_data_to_list(data_list: List[Dict]) -> List[List]:
    """
    将字典格式数据转换为列表格式（适配ODPS写入要求）
    严格按表字段顺序组装，确保字段映射正确
    """
    write_data = []
    for data in data_list:
        row = []
        for col in CONFIG["odps"]["table_columns"]:
            row.append(data.get(col, ""))
        write_data.append(row)
    return write_data


# ===================== 主流程（分批采集+分批写入+时间统计+并行采集） =====================
def main():
    """主执行流程：Token获取 → 采集数据 → 分批写入ODPS + 任务时间统计 + 并行采集"""
    # 初始化任务时间统计
    task_start_time = datetime.now()  # 任务开始时间（datetime对象，用于计算时长）
    task_start_str = task_start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 格式化到毫秒

    try:
        print("=" * 100)
        print("🚀 秒针广告位详情采集+ODPS写入任务启动")
        print(f"📅 任务开始时间：{task_start_str}")
        print(
            f"🔧 全局ODPS项目：{ODPS_PROJECT} | 批量写入大小：{CONFIG['batch_size']}条/批 | 接口并行度：{CONFIG['api']['parallelism']}")
        print("=" * 100)

        # 1. 前置校验：ODPS项目是否有效
        if not ODPS_PROJECT:
            raise Exception("❌ 全局ODPS_PROJECT变量获取失败，请检查ODPS环境配置")

        # 2. 获取Access Token
        token = get_access_token()

        # 3. 获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            print("⚠️ 未获取到任何campaign_id，任务终止")
            return

        # 4. 遍历采集数据 + 分批写入
        all_spot_detail_data = []
        target_table = CONFIG["odps"]["table_name"]
        # 补充dt参数（需确保外部传入args，若为命令行执行需补充argparse，此处保持原有逻辑）
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--dt', required=True, help='分区日期，格式如20240101')
        args = parser.parse_args()
        partition_dt = args.dt

        for campaign_id in campaign_ids:
            # 获取当前campaign的spot_id_str列表
            spot_id_str_list = get_spot_id_str_list(token, campaign_id)
            if not spot_id_str_list:
                continue

            # 批量并行采集每个spot的详情（并行度=10）
            spot_detail_list = batch_get_spot_detail(token, campaign_id, spot_id_str_list)
            if spot_detail_list:
                all_spot_detail_data.extend(spot_detail_list)

                # 达到批量大小则写入ODPS
                if len(all_spot_detail_data) >= CONFIG["batch_size"]:
                    write_data = convert_data_to_list(all_spot_detail_data)
                    write_to_odps(target_table, write_data, partition_dt)
                    all_spot_detail_data = []  # 清空缓存，准备下一批

        # 5. 写入剩余数据（不足1000条的部分）
        if all_spot_detail_data:
            write_data = convert_data_to_list(all_spot_detail_data)
            write_to_odps(target_table, write_data, partition_dt)

        # 计算任务耗时
        task_end_time = datetime.now()
        task_end_str = task_end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        task_duration = (task_end_time - task_start_time).total_seconds()  # 总时长（秒）
        # 格式化时长（分:秒）
        duration_min = int(task_duration // 60)
        duration_sec = round(task_duration % 60, 3)

        # 任务完成提示
        print("\n" + "=" * 100)
        print(f"✅ 任务全部完成！")
        print(f"📅 任务开始时间：{task_start_str}")
        print(f"📅 任务结束时间：{task_end_str}")
        print(f"⏱️  任务总时长：{duration_min}分{duration_sec}秒（{task_duration:.3f}秒）")
        print(f"📌 数据写入位置：{ODPS_PROJECT}.{target_table}（分区：dt='{partition_dt}'）")
        print(f"📌 核心说明：仅首次写入前清空分区，全量数据已保留 | 接口并行度：{CONFIG['api']['parallelism']}")
        print("=" * 100)

    except Exception as e:
        # 异常时也统计耗时
        task_end_time = datetime.now()
        task_end_str = task_end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        task_duration = (task_end_time - task_start_time).total_seconds()
        duration_min = int(task_duration // 60)
        duration_sec = round(task_duration % 60, 3)

        print(f"\n❌ 任务执行失败！")
        print(f"📅 任务开始时间：{task_start_str}")
        print(f"📅 任务结束时间：{task_end_str}")
        print(f"⏱️  任务耗时：{duration_min}分{duration_sec}秒（{task_duration:.3f}秒）")
        print(f"❌ 失败原因：{str(e)}")
        print("=" * 100)
        raise  # 抛出异常，便于排查问题


if __name__ == "__main__":
    main()