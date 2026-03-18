# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime
import time
import urllib3
from odps import ODPS

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    # API配置
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "regions_list_url": "https://api.cn.miaozhen.com/cms/v1/regions/list",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",  # 替换为实际账号
            "password": "Coachapi2026",  # 替换为实际密码
            "client_id": "COACH2026_API",  # 替换为实际client_id
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"  # 替换为实际secret
        },
        "timeout": 30,
        "interval": 0.1  # 接口调用间隔
    },
    # ODPS配置
    "table_name": "ods_mz_adm_cms_regions_list_api_df",
    "batch_size": 1000,
    "partition_date": datetime.now().strftime("%Y%m%d"),  # 分区日期(yyyyMMdd)
    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 落地时间(yyyy-MM-dd HH:mm:ss)
}

# 自动获取ODPS项目名
try:
    ODPS_PROJECT = ODPS().project
    if not ODPS_PROJECT:
        raise Exception("ODPS项目名自动获取失败，请检查ODPS客户端配置")
    print(f"✅ 自动获取ODPS项目名：{ODPS_PROJECT}")
except Exception as e:
    raise Exception(f"ODPS初始化失败：{str(e)}")


# ===================== 通用工具函数 =====================
def safe_str(val):
    """安全转换字符串，空值返回None"""
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    if isinstance(val, int):
        return str(val)
    return str(val)


def get_miaozhen_token():
    """获取秒针access_token"""
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
        raise Exception(f"获取Token失败：{str(e)}")


# ===================== 核心API调用函数 =====================
def get_regions_detail(token):
    """调用regions/list接口获取地区列表"""
    print("🔍 调用/cms/v1/regions/list接口获取地区数据...")
    try:
        params = {"access_token": token}
        resp = requests.get(
            CONFIG["api"]["regions_list_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        regions_data = resp.json().get("result", {}).get("regions", [])

        # 标准化输出字段
        standard_regions_list = []
        for region in regions_data:
            if not isinstance(region, dict):
                continue
            standard_region = {
                "level": safe_str(region.get("level")),
                "parent_id": safe_str(region.get("parent_id")),
                "region_id": safe_str(region.get("region_id")),
                "region_name": safe_str(region.get("region_name")),
                # 补充关联字段
                "pre_parse_raw_text": json.dumps(region, ensure_ascii=False),
                "etl_time": CONFIG["etl_date"]
            }
            standard_regions_list.append(standard_region)

        print(f"✅ 获取到{len(standard_regions_list)}条地区数据")
        return standard_regions_list
    except Exception as e:
        raise Exception(f"获取地区数据失败：{str(e)}")


# ===================== ODPS操作函数 =====================
def init_odps_client():
    """初始化ODPS客户端"""
    try:
        odps = ODPS(project=ODPS_PROJECT)
        print(f"✅ ODPS客户端初始化成功")
        return odps
    except Exception as e:
        raise Exception(f"ODPS客户端初始化失败：{str(e)}")


def write_regions_data_to_odps(odps, regions_data_list):
    """批量写入地区数据到ODPS"""
    if not regions_data_list:
        print(f"⚠️ 无有效地区数据，跳过ODPS写入")
        return

    # 构造写入数据（按表字段顺序）
    write_data = []
    for region in regions_data_list:
        row = [
            region["level"],
            region["parent_id"],
            region["region_id"],
            region["region_name"],
            region["pre_parse_raw_text"],
            region["etl_time"]
        ]
        write_data.append(row)

    try:
        # 校验表是否存在
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"ODPS表不存在：{CONFIG['table_name']}，请先创建表后再运行脚本")

        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{CONFIG['partition_date']}'"

        # 清空当日分区（可选）
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {CONFIG['table_name']} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 分批写入
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            for i in range(0, len(write_data), CONFIG["batch_size"]):
                batch_data = write_data[i:i + CONFIG["batch_size"]]
                writer.write(batch_data)
                print(f"🔸 写入批次{i // CONFIG['batch_size'] + 1} | 条数：{len(batch_data)}")

        print(f"✅ ODPS写入完成 | 总条数：{len(write_data)} | 分区：{CONFIG['partition_date']}")
    except Exception as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针地区列表数据采集任务启动")
    print(f"📅 分区日期：{CONFIG['partition_date']}")
    print(f"📅 落地时间：{CONFIG['etl_date']}")
    print(f"📋 目标表名：{CONFIG['table_name']}")
    print("=" * 80)

    try:
        # 1. 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 2. 获取地区列表数据
        regions_data_list = get_regions_detail(token)

        # 3. 写入ODPS
        write_regions_data_to_odps(odps, regions_data_list)

        # 4. 任务完成统计
        print("\n" + "=" * 80)
        print("✅ 地区列表采集任务完成！")
        print(f"📊 采集地区总数：{len(regions_data_list)}条")
        print(f"📊 数据写入表：{CONFIG['table_name']}（dt={CONFIG['partition_date']}）")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()