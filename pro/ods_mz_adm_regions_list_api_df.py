# -*- coding: utf-8 -*-
"""
秒针区域列表采集脚本
MaxCompute认证：极简免密初始化（ODPS().project）
功能：采集数据并写入MaxCompute（替代打印）
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS, Record  # 导入MaxCompute SDK

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. API配置
API_CONFIG = {
    "token_url": "https://api.cn.miaozhen.com/oauth/token",
    "regions_url": "https://api.cn.miaozhen.com/cms/v1/regions/list",
    "auth": {
        "grant_type": "password",
        "username": "Coach_api",
        "password": "Coachapi2026",
        "client_id": "COACH2026_API",
        "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
    },
    "timeout": 30,
    "interval": 0.2
}

# 2. MaxCompute极简配置（仅保留表名和批量大小）
MAXCOMPUTE_CONFIG = {
    "table_name": "ods_mz_adm_regions_list_api_df",  # 目标表名
    "batch_size": 1000  # 批量写入大小
}


# ===================== 工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


# ===================== MaxCompute极简初始化（核心修改） =====================
def init_maxcompute() -> Optional[ODPS]:
    """
    极简免密初始化MaxCompute客户端
    """
    try:
        # 核心修改：仅初始化ODPS对象，不传入任何参数
        o = ODPS(project=ODPS().project)

        # 验证连接（列出表名，确认认证成功）
        o.list_tables(max_items=1)
        print(f"✅ MaxCompute极简初始化成功（默认项目：{ODPS().project}）")
        return o
    except Exception as e:
        print(f"❌ MaxCompute极简初始化失败：{str(e)}")
        return None


# ===================== Token获取 =====================
def get_access_token() -> Optional[str]:
    """获取Access Token（标准OAuth2密码模式）"""
    try:
        auth_params = API_CONFIG["auth"]
        resp = requests.post(
            API_CONFIG["token_url"],
            data=auth_params,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()

        access_token = token_data.get("access_token")
        if not access_token:
            raise Exception(f"Token返回无access_token字段，返回数据：{token_data}")

        print(f"✅ Access Token获取成功（前10位：{access_token[:10]}...）")
        return access_token

    except Exception as e:
        print(f"❌ 获取Access Token失败：{str(e)}")
        return None


# ===================== 区域列表采集 =====================
def get_regions_list(token: str) -> List[Dict]:
    """采集区域列表数据（URL参数传递Token）"""
    if not token:
        raise Exception("认证Token为空，无法调用接口")

    try:
        params = {"access_token": token}
        headers = {"Content-Type": "application/json"}

        resp = requests.get(
            API_CONFIG["regions_url"],
            params=params,
            headers=headers,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        regions_data = resp.json()

        if not isinstance(regions_data, list):
            raise Exception(f"区域列表返回格式异常（非数组），返回数据：{regions_data}")

        etl_datetime = get_etl_datetime()
        standard_regions_list = []

        # 解析数据
        for idx, region in enumerate(regions_data):
            if not isinstance(region, dict):
                print(f"⚠️ 第{idx + 1}条区域数据非字典格式，跳过：{region}")
                continue

            standard_region = {
                "level": to_string(region.get("level")),
                "parent_id": to_string(region.get("parent_id")),
                "region_id": to_string(region.get("region_id")),
                "region_name": to_string(region.get("region_name")),
                "pre_parse_raw_text": to_string(json.dumps(region, ensure_ascii=False)),
                "etl_datetime": to_string(etl_datetime)
            }
            standard_regions_list.append(standard_region)

        time.sleep(API_CONFIG["interval"])
        print(f"✅ 成功解析{len(standard_regions_list)}条有效区域数据（接口总计返回{len(regions_data)}条）")
        return standard_regions_list

    except requests.exceptions.HTTPError as e:
        error_detail = f"HTTP错误 {e.response.status_code}：{e.response.text}"
        raise Exception(f"接口调用失败：{error_detail}")
    except Exception as e:
        raise Exception(f"采集区域列表失败：{str(e)}")


# ===================== 写入MaxCompute（核心逻辑） =====================
def batch_write_to_maxcompute(odps_client: ODPS, data: List[Dict]):
    """批量写入数据到MaxCompute（极简认证版）"""
    if not odps_client:
        raise Exception("MaxCompute客户端未初始化，无法写入数据")
    if not data:
        raise Exception("无有效数据可写入MaxCompute")

    try:
        table_name = MAXCOMPUTE_CONFIG["table_name"]
        batch_size = MAXCOMPUTE_CONFIG["batch_size"]

        # 1. 检查表是否存在
        if not odps_client.exist_table(table_name):
            raise Exception(f"MaxCompute表 {table_name} 不存在，请先创建表")

        # 2. 获取表结构
        table = odps_client.get_table(table_name)
        schema = table.schema

        # 3. 批量写入数据
        write_count = 0
        records = []
        for idx, row in enumerate(data):
            # 构造Record（匹配表字段）
            record = Record(schema=schema)
            record["level"] = row["level"]
            record["parent_id"] = row["parent_id"]
            record["region_id"] = row["region_id"]
            record["region_name"] = row["region_name"]
            record["pre_parse_raw_text"] = row["pre_parse_raw_text"]
            record["etl_datetime"] = row["etl_datetime"]

            records.append(record)
            write_count += 1

            # 达到批量大小则写入
            if len(records) >= batch_size:
                with table.open_writer(partition=None) as writer:
                    writer.write(records)
                print(f"✅ 批量写入{len(records)}条数据（累计：{write_count}）")
                records = []

        # 写入剩余数据
        if records:
            with table.open_writer(partition=None) as writer:
                writer.write(records)
            print(f"✅ 写入剩余{len(records)}条数据（累计：{write_count}）")

        print(f"🎉 数据写入MaxCompute完成，总计写入{write_count}条数据到表 {table_name}")

    except Exception as e:
        raise Exception(f"写入MaxCompute失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    """脚本主入口"""
    print(f"🚀 开始执行秒针区域列表采集脚本（{get_etl_datetime()}）")

    try:
        # 1. 极简初始化MaxCompute客户端
        odps_client = init_maxcompute()
        if not odps_client:
            print("⚠️ MaxCompute初始化失败，任务终止")
            return

        # 2. 获取Token
        token = get_access_token()
        if not token:
            print("⚠️ Token获取失败，任务终止")
            return

        # 3. 采集数据
        regions_list_data = get_regions_list(token)
        if not regions_list_data:
            print("⚠️ 未采集到任何有效区域数据，任务终止")
            return

        # 4. 写入MaxCompute（替换原打印逻辑）
        batch_write_to_maxcompute(odps_client, regions_list_data)

        print(f"✅ 脚本执行完成（{get_etl_datetime()}）")

    except Exception as e:
        print(f"❌ 脚本执行失败：{str(e)}")


if __name__ == "__main__":
    main()