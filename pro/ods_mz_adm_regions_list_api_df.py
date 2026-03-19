# -*- coding: utf-8 -*-
"""
秒针区域列表采集脚本
数据来源：/cms/v1/regions/list 接口
目标表：ods_mz_adm_regions_list_api_df
配置方式：统一CONFIG字典管理
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from odps import ODPS

# ===================== 统一配置（按指定格式改造） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "regions_url": "https://api.cn.miaozhen.com/cms/v1/regions/list",  # 区域列表接口
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 30,
        "interval": 0.2  # 接口调用间隔
    },
    "table_name": "ods_mz_adm_regions_list_api_df",  # 指定目标表名
    "batch_size": 1000  # ODPS批量写入大小
}


# ===================== 核心工具函数 =====================
def get_etl_datetime() -> str:
    """获取当前时间戳（yyyy-MM-dd HH:mm:ss）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """强制转换为字符串，处理空值/None"""
    if value is None or value == "" or str(value).lower() == "null":
        return ""
    return str(value)


# ===================== Token获取（适配CONFIG配置） =====================
def get_access_token() -> str:
    """获取Access Token（使用CONFIG中的认证参数）"""
    try:
        # 从CONFIG中读取认证参数
        auth_params = CONFIG["api"]["auth"]
        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=auth_params,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        token_data = resp.json()

        # 提取access_token
        access_token = token_data.get("access_token")
        if not access_token:
            raise Exception(f"Token返回无access_token字段，返回数据：{token_data}")

        print(f"✅ Access Token获取成功（前10位：{access_token[:10]}...）")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Access Token失败：{str(e)}")


# ===================== 区域列表采集（适配CONFIG配置） =====================
def get_regions_list(token: str) -> List[Dict]:
    """采集/cms/v1/regions/list接口的所有区域数据"""
    try:
        # 构造请求头（携带Token）
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # 调用区域列表接口（从CONFIG读取URL）
        resp = requests.get(
            CONFIG["api"]["regions_url"],
            headers=headers,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        regions_data = resp.json()

        # 校验返回格式为数组
        if not isinstance(regions_data, list):
            raise Exception(f"区域列表返回格式异常（非数组），返回数据：{regions_data}")

        etl_datetime = get_etl_datetime()
        standard_regions_list = []

        # 解析每条区域数据
        for idx, region in enumerate(regions_data):
            if not isinstance(region, dict):
                print(f"⚠️ 第{idx + 1}条区域数据非字典格式，跳过：{region}")
                continue

            standard_region = {
                # 接口返回字段
                "level": to_string(region.get("level")),
                "parent_id": to_string(region.get("parent_id")),
                "region_id": to_string(region.get("region_id")),
                "region_name": to_string(region.get("region_name")),
                # 溯源字段
                "pre_parse_raw_text": to_string(json.dumps(region, ensure_ascii=False)),
                "etl_datetime": to_string(etl_datetime)
            }
            standard_regions_list.append(standard_region)

        # 接口调用间隔（从CONFIG读取）
        time.sleep(CONFIG["api"]["interval"])
        print(f"✅ 成功解析{len(standard_regions_list)}条区域数据（总计返回{len(regions_data)}条）")
        return standard_regions_list
    except Exception as e:
        raise Exception(f"采集区域列表失败：{str(e)}")


# ===================== ODPS批量写入（适配batch_size配置） =====================
def batch_write_to_odps(data: List[List], dt: str):
    """ODPS批量写入（按CONFIG中的batch_size分批次）"""
    if not data:
        print(f"⚠️ 无有效数据写入ODPS，任务终止")
        return

    try:
        # 初始化ODPS客户端
        o = ODPS()
        table_name = CONFIG["table_name"]
        table = o.get_table(table_name)
        partition_spec = f"dt='{dt}'"
        batch_size = CONFIG["batch_size"]

        # 清空分区
        if table.exist_partition(partition_spec):
            o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{table_name} {partition_spec}")

        # 分批次写入
        total_count = len(data)
        batches = [data[i:i + batch_size] for i in range(0, total_count, batch_size)]

        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for batch_idx, batch in enumerate(batches):
                writer.write(batch)
                print(f"✅ 写入第{batch_idx + 1}/{len(batches)}批次，条数：{len(batch)}")

        print(f"🎉 数据全部写入完成：{table_name} | 分区{dt} | 总条数{total_count}")
    except Exception as e:
        raise Exception(f"ODPS批量写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    try:
        # 1. 获取Access Token
        token = get_access_token()

        # 2. 采集区域列表数据
        regions_list_data = get_regions_list(token)
        if not regions_list_data:
            print("⚠️ 未采集到任何有效区域数据，任务终止")
            return

        # 3. 格式化数据（与表字段顺序一致）
        regions_write_data = [
            [
                t["level"],
                t["parent_id"],
                t["region_id"],
                t["region_name"],
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in regions_list_data
        ]

        # 4. 批量写入ODPS（分区日期可替换为动态参数args['dt']）
        batch_write_to_odps(regions_write_data, '20260318')

    except Exception as e:
        print(f"❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()