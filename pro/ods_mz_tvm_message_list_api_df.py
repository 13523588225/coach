import json
import time
import argparse
from datetime import datetime
from typing import Dict, List
import requests
import urllib3
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "message_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/config/message/list",
    "timeout": 30,
    "request_interval": 0.2,
    "page_size": 50,
    "types": ["publisher", "region"]
}

# 2. ODPS配置
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "message": "ods_mz_tvm_message_list_api_df"
}


# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    if value is None or value == "" or value == "null":
        return ""
    return str(value)


def get_log() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 从MaxCompute查询API账号密码 =====================
def get_tvm_api_credentials():
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'TVM' limit 1 \
          """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            return record["username"], record["passwords"]
    except errors.ODPSError as e:
        raise Exception(f"查询账号密码失败：{str(e)}")


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    try:
        username, password = get_tvm_api_credentials()
        resp = requests.post(
            API_CONFIG["token_url"],
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()
        if token_data.get("error_code") != 0:
            raise Exception(f"Token错误：{token_data.get('error_message')}")
        return to_string(token_data.get("result", {}).get("access_token"))
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_message_list(token: str) -> List[Dict]:
    """适配接口返回结构：result = [{"id":1,"name":"xx"}, ...]"""
    all_data = []
    page_size = API_CONFIG["page_size"]
    etl_datetime = get_etl_datetime()
    request_types = API_CONFIG["types"]

    try:
        # 遍历 type: publisher / region
        for data_type in request_types:
            print(f"\n[{get_log()}] 🎯 开始采集 type={data_type}")

            # 1. 请求第一页获取分页总数
            first_url = (
                f"{API_CONFIG['message_list_url']}?access_token={token}"
                f"&pageSize={page_size}&pageNo=1&type={data_type}"
            )
            resp = requests.get(first_url, timeout=API_CONFIG["timeout"], verify=False)
            resp.raise_for_status()
            response_data = resp.json()

            if response_data.get("error_code") != 0:
                raise Exception(f"接口错误：{response_data.get('error_message')}")

            # 核心：接口返回 result 是数组，直接取值
            result_list = response_data.get("result", [])
            total_page_no = response_data.get("totalPageNo", 1)
            print(f"[{get_log()}] 📊 type={data_type} | 总页数：{total_page_no}")

            # 2. 遍历所有分页
            for page_no in range(1, total_page_no + 1):
                full_url = (
                    f"{API_CONFIG['message_list_url']}?access_token={token}"
                    f"&pageSize={page_size}&pageNo={page_no}&type={data_type}"
                )
                print(f"[{get_log()}] 📄 请求第 {page_no}/{total_page_no} 页")

                resp = requests.get(full_url, timeout=API_CONFIG["timeout"], verify=False)
                resp.raise_for_status()
                response_data = resp.json()
                items = response_data.get("result", [])

                # 严格按照指定字段构造数据
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    all_data.append({
                        "request_type": to_string(data_type),  # 数据类型 publisher/region
                        "id": to_string(item.get("id")),  # 配置ID
                        "region_name": to_string(item.get("name")),  # 配置名称
                        "full_request_url": to_string(full_url),  # 完整请求URL
                        "pre_parse_raw_text": to_string(json.dumps(item, ensure_ascii=False)),  # 原始JSON
                        "etl_datetime": etl_datetime  # 数据落地时间
                    })
                time.sleep(API_CONFIG["request_interval"])

        print(f"\n[{get_log()}] ✅ 采集完成，总数据量：{len(all_data)}")
        return all_data

    except Exception as e:
        raise Exception(f"采集失败：{str(e)}")


# ===================== ODPS写入 =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    if not data:
        print(f"⚠️ 无数据写入")
        return
    o = ODPS(project=ODPS_PROJECT)
    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")

    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入ODPS成功：{len(data)} 条")


# ===================== 主流程 =====================
def main():
    try:
        token = get_miaozhen_token()
        message_data = get_message_list(token)

        # 【严格匹配表字段顺序】写入数据
        write_data = [[
            item["request_type"],
            item["id"],
            item["region_name"],
            item["full_request_url"],
            item["pre_parse_raw_text"],
            item["etl_datetime"]
        ] for item in message_data]

        write_to_odps(TABLE_NAMES["message"], write_data, args['dt'])
    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()