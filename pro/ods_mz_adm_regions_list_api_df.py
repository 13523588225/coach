# -*- coding: utf-8 -*-
"""
秒针区域列表采集脚本
整合通用ODPS写入函数（分区写入+清空分区）
MaxCompute初始化：ODPS(project=ODPS_PROJECT)
"""
import requests
import json
import time
import urllib3
import argparse  # 新增：解析命令行dt参数
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS, errors
from urllib.parse import urlencode

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. API配置（新增lang列表、分页步长）
API_CONFIG = {
    "token_url": "https://api.cn.miaozhen.com/oauth/token",
    "regions_url": "https://api.cn.miaozhen.com/cms/v1/regions/list",
    "auth": {
        "grant_type": "password",
        "username": "",  # 改为空，从数仓获取
        "password": "",  # 改为空，从数仓获取
        "client_id": "COACH2026_API",
        "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
    },
    "timeout": 30,
    "interval": 0.2,
    "lang_list": ["en", "cn"],  # 新增：要采集的语言列表
    "page_step": 2000  # 新增：分页步长（每次请求2000条）
}

# 2. ODPS全局配置（需替换为实际项目名）
ODPS_PROJECT = ODPS().project
TARGET_TABLE_NAME = "ods_mz_adm_regions_list_api_df"  # 目标表名（需新增lang字段）


# ===================== 工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


def get_log() -> str:
    """获取日志时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 从MaxCompute查询API账号密码 =====================
def get_adm_api_credentials():
    """
    从数仓表 ods_mz_user_api_df 查询ADM接口的账号密码
    SQL: select username,passwords from ods_mz_user_api_df where api_source ='ADM'
    """
    o = ODPS(project=ODPS_PROJECT)
    sql = """
    select username, passwords 
    from ods_mz_user_api_df 
    where api_source = 'ADM'
    limit 1
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


# ===================== 通用ODPS写入函数（完全复用原有版本） =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    """通用ODPS写入函数（清空分区+写入）"""
    if not data:
        print(f"⚠️ {table_name} 无数据可写入")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表{table_name}不存在")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    # 清空分区（防重复）
    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{table_name}.{partition_spec}")

    # 写入数据
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入成功：{table_name} | 分区{dt} | 条数{len(data)}")


# ===================== Token获取（新增从数仓加载账号密码逻辑） =====================
def get_access_token() -> Optional[str]:
    """获取Access Token（标准OAuth2密码模式）"""
    try:
        # 从数仓获取账号密码并填充
        username, password = get_adm_api_credentials()
        auth_params = API_CONFIG["auth"]
        auth_params["username"] = username
        auth_params["password"] = password

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


# ===================== 区域列表采集（核心修改：多语言+分页获取所有数据） =====================
def get_regions_list(token: str) -> List[Dict]:
    """采集区域列表数据（支持多语言、分页获取所有数据）"""
    if not token:
        raise Exception("认证Token为空，无法调用接口")

    all_regions_data = []  # 存储所有语言+所有分页的最终数据
    etl_datetime = get_etl_datetime()  # 统一ETL时间戳

    # 遍历所有语言（en/cn）
    for lang in API_CONFIG["lang_list"]:
        offset = 0  # 分页偏移量，从0开始
        step = API_CONFIG["page_step"]
        print(f"\n[{get_log()}] 📢 开始采集【{lang}】语言的区域数据（分页步长：{step}）")

        while True:
            try:
                # 构造请求参数（包含token、lang、limit分页）
                params = {
                    "access_token": token,
                    "lang": lang,
                    "limit": f"{offset},{step}"
                }
                headers = {"Content-Type": "application/json"}

                # 拼接完整请求URL（包含所有参数）
                full_request_url = f"{API_CONFIG['regions_url']}?{urlencode(params)}"

                # 发送分页请求
                resp = requests.get(
                    API_CONFIG["regions_url"],
                    params=params,
                    headers=headers,
                    timeout=API_CONFIG["timeout"],
                    verify=False
                )
                resp.raise_for_status()
                page_regions = resp.json()

                # 校验返回格式
                if not isinstance(page_regions, list):
                    raise Exception(f"【{lang}】第{offset//step + 1}页返回非数组：{page_regions}")

                page_data_len = len(page_regions)
                print(f"[{get_log()}] 📄 【{lang}】第{offset//step + 1}页：获取{page_data_len}条数据（offset={offset}）")

                # 解析当前页数据
                for idx, region in enumerate(page_regions):
                    if not isinstance(region, dict):
                        print(f"⚠️ 【{lang}】第{offset//step + 1}页第{idx+1}条非字典，跳过：{region}")
                        continue

                    # 标准化数据（新增lang字段）
                    standard_region = {
                        "level": to_string(region.get("level")),
                        "parent_id": to_string(region.get("parent_id")),
                        "region_id": to_string(region.get("region_id")),
                        "region_name": to_string(region.get("region_name")),
                        "lang": to_string(lang),  # 新增：语言标识
                        "full_request_url": to_string(full_request_url),  # 含lang+limit的完整URL
                        "pre_parse_raw_text": to_string(json.dumps(region, ensure_ascii=False)),
                        "etl_datetime": to_string(etl_datetime)
                    }
                    all_regions_data.append(standard_region)

                # 分页终止条件：当前页数据量 < 步长 → 无更多数据
                if page_data_len < step:
                    print(f"[{get_log()}] 🎯 【{lang}】语言采集完成，累计{len(all_regions_data)}条（全局）")
                    break

                # 偏移量累加，准备下一页
                offset += step
                time.sleep(API_CONFIG["interval"])  # 请求间隔，避免高频

            except requests.exceptions.HTTPError as e:
                error_detail = f"【{lang}】第{offset//step + 1}页HTTP错误 {e.response.status_code}：{e.response.text}"
                raise Exception(f"接口调用失败：{error_detail}")
            except Exception as e:
                raise Exception(f"【{lang}】语言采集失败：{str(e)}")

    print(f"\n✅ 所有语言采集完成，总计有效数据：{len(all_regions_data)}条")
    return all_regions_data


# ===================== 主流程（新增dt参数解析） =====================
def main():

    print(f"🚀 开始执行秒针区域列表采集脚本（{get_etl_datetime()}）")

    try:
        # 1. 获取Token
        token = get_access_token()
        if not token:
            print("⚠️ Token获取失败，任务终止")
            return

        # 2. 采集多语言+全量分页数据
        regions_list_data = get_regions_list(token)
        if not regions_list_data:
            print("⚠️ 未采集到任何有效区域数据，任务终止")
            return

        # 3. 格式化数据（新增lang字段）
        regions_write_data = [
            [
                t["level"],
                t["parent_id"],
                t["region_id"],
                t["region_name"],
                t["lang"],  # 新增lang字段写入
                t["full_request_url"],
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in regions_list_data
        ]

        # 4. 写入ODPS
        write_to_odps(TARGET_TABLE_NAME, regions_write_data, args['dt'])

        print(f"\n✅ 脚本执行完成（{get_etl_datetime()}）")

    except Exception as e:
        print(f"\n❌ 脚本执行失败：{str(e)}")
        raise  # 抛出异常，便于调度平台感知


if __name__ == "__main__":
    main()