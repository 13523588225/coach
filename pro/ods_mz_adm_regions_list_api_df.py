# -*- coding: utf-8 -*-
"""
秒针区域列表采集脚本 - 终极适配版
严格适配官方API：limit=偏移量,获取条数
无无限循环、无参数错误、稳定写入ODPS
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS, errors
from urllib.parse import urlencode

# ===================== 全局配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. API配置（严格适配你提供的接口格式）
API_CONFIG = {
    "token_url": "https://api.cn.miaozhen.com/oauth/token",
    "regions_url": "https://api.cn.miaozhen.com/cms/v1/regions/list",
    "auth": {
        "grant_type": "password",
        "username": "",
        "password": "",
        "client_id": "COACH2026_API",
        "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
    },
    "timeout": 30,
    "interval": 0.2,
    "lang_list": ["en", "cn"],
    "page_size": 1999,    # ✅ 仅修复这里：2000 → 1999（匹配你的API注释）
    "max_page": 10      # 最大分页兜底，防死循环
}

# 2. ODPS全局配置
ODPS_PROJECT = ODPS().project
TARGET_TABLE_NAME = "ods_mz_adm_regions_list_api_df"


# ===================== 工具函数 =====================
def get_etl_datetime() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def to_string(value) -> str:
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)

def get_log() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 获取API账号 =====================
def get_adm_api_credentials():
    o = ODPS(project=ODPS_PROJECT)
    sql = """
        select username, passwords
        from ods_mz_user_api_df
        where api_source = 'ADM' limit 1
    """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_log()}] 🔐 成功获取ADM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"查询账号失败：{str(e)}")


# ===================== ODPS写入函数 =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    if not data:
        print(f"⚠️ {table_name} 无数据")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表{table_name}不存在")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    # 清空旧分区
    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{partition_spec}")

    # 写入新数据
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入成功：{len(data)}条 | 分区dt={dt}")


# ===================== 获取Token =====================
def get_access_token() -> Optional[str]:
    try:
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
            raise Exception("未获取到access_token")
        print(f"✅ Token获取成功：{access_token[:10]}...")
        return access_token

    except Exception as e:
        print(f"❌ Token失败：{str(e)}")
        return None


# ===================== 核心采集（100%适配你的API格式） =====================
def get_regions_list(token: str) -> List[Dict]:
    if not token:
        raise Exception("Token为空")

    all_data = []
    etl_time = get_etl_datetime()

    for lang in API_CONFIG["lang_list"]:
        offset = 0
        page_num = 0
        page_size = API_CONFIG["page_size"]
        print(f"\n[{get_log()}] 📢 开始采集【{lang}】语言数据")

        while True:
            page_num += 1
            # 兜底保护
            if page_num > API_CONFIG["max_page"]:
                print(f"⚠️ 超过最大分页，终止【{lang}】采集")
                break

            try:
                # ✅ 终极适配：和你提供的URL格式完全一致 limit=offset,count
                params = {
                    "access_token": token,
                    "lang": lang,
                    "limit": f"{offset},{page_size}"
                }
                headers = {"Content-Type": "application/json"}
                req_url = f"{API_CONFIG['regions_url']}?{urlencode(params)}"

                # 请求接口
                resp = requests.get(
                    API_CONFIG["regions_url"],
                    params=params,
                    headers=headers,
                    timeout=API_CONFIG["timeout"],
                    verify=False
                )
                resp.raise_for_status()
                data_list = resp.json()

                if not isinstance(data_list, list):
                    raise Exception("返回数据非数组")

                current_count = len(data_list)
                print(f"[{get_log()}] 📄 【{lang}】第{page_num}页：获取{current_count}条")

                # 数据标准化
                for item in data_list:
                    if isinstance(item, dict):
                        all_data.append({
                            "request_lang": to_string(lang),
                            "s_level": to_string(item.get("level")),
                            "parent_id": to_string(item.get("parent_id")),
                            "region_id": to_string(item.get("region_id")),
                            "region_name": to_string(item.get("region_name")),
                            "full_request_url": to_string(req_url),
                            "pre_parse_raw_text": to_string(json.dumps(item, ensure_ascii=False)),
                            "etl_datetime": to_string(etl_time)
                        })

                # ✅ 终止条件：无数据 或 不足一页
                if current_count == 0 or current_count < page_size:
                    print(f"[{get_log()}] 🎯 【{lang}】采集完成，共{page_num}页")
                    break

                # 偏移量递增
                offset += page_size
                time.sleep(API_CONFIG["interval"])

            except Exception as e:
                raise Exception(f"第{page_num}页采集失败：{str(e)}")

    print(f"\n✅ 全量采集完成，总数据：{len(all_data)}条")
    return all_data


# ===================== 主函数 =====================
def main():
    print(f"🚀 脚本启动：{get_etl_datetime()}")
    try:
        token = get_access_token()
        if not token:
            return

        data = get_regions_list(token)
        if not data:
            return

        # 格式化写入
        write_data = [[
            i["request_lang"], i["s_level"], i["parent_id"],
            i["region_id"], i["region_name"], i["full_request_url"],
            i["pre_parse_raw_text"], i["etl_datetime"]
        ] for i in data]

        # 👉 100%保留你的代码，未做任何修改
        write_to_odps(TARGET_TABLE_NAME, write_data, args['dt'])
        print(f"\n🎉 脚本执行成功！")

    except Exception as e:
        print(f"\n❌ 脚本失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()