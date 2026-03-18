# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime
import time
import urllib3
from odps import ODPS, options

# ===================== 基础配置（ODPS自动获取） =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG = {
    # API配置
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "campaign_list_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "campaign_list_targets_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_targets",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",  # 替换为实际账号
            "password": "Coachapi2026",  # 替换为实际密码
            "client_id": "COACH2026_API",  # 替换为实际client_id
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"  # 替换为实际secret
        },
        "timeout": 30,
        "interval": 0.1  # 接口调用间隔，避免限流
    },
    # ODPS配置（最终表名）
    "table_name": "ods_mz_adm_cms_list_targets_api_df",
    "batch_size": 1000,
    "etl_date": datetime.now().strftime("%Y%m%d")  # 分区日期
}

# 自动获取ODPS项目名
try:
    ODPS_PROJECT = ODPS().project
    if not ODPS_PROJECT:
        raise Exception("ODPS项目名自动获取失败，请检查ODPS客户端配置")
    print(f"✅ 自动获取ODPS项目名：{ODPS_PROJECT}")
except Exception as e:
    raise Exception(f"ODPS项目初始化失败：{str(e)}")


# ===================== 通用工具函数 =====================
def safe_str(val):
    """安全转换字符串，空值返回None"""
    if val is None or val == "" or val in ("null", "undefined"):
        return None
    # 特殊处理数值类型
    if isinstance(val, (int, float)):
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
        raise Exception(f"Token获取失败：{str(e)}")


# ===================== 核心API调用函数 =====================
def get_campaign_ids(token):
    """从list接口获取campaign_id列表"""
    print("🔍 获取活动ID列表（/cms/v1/campaigns/list）...")
    try:
        resp = requests.get(
            f"{CONFIG['api']['campaign_list_url']}?access_token={token}",
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()
        campaigns = raw_data.get("result", {}).get("campaigns", [])

        # 提取有效campaign_id
        campaign_ids = []
        for camp in campaigns:
            if isinstance(camp, dict) and camp.get("campaign_id"):
                campaign_ids.append(str(camp.get("campaign_id")))

        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        return campaign_ids
    except Exception as e:
        raise Exception(f"获取campaign_id失败：{str(e)}")


def get_target_detail(token, campaign_id):
    """调用list_targets接口获取指定campaign_id的目标人群详情"""
    print(f"  🔍 获取活动{campaign_id}的目标人群（/cms/v1/campaigns/list_targets）...")
    try:
        params = {
            "access_token": token,
            "campaign_id": campaign_id
        }
        resp = requests.get(
            CONFIG["api"]["campaign_list_targets_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        target_detail = resp.json().get("result", {})
        targets = target_detail.get("targets", [])

        # 标准化输出字段（严格匹配指定结构）
        standard_target_list = []
        for target in targets:
            if not isinstance(target, dict):
                continue
            standard_target = {
                "panel_id": safe_str(target.get("panel_id")),
                "target_id": safe_str(target.get("target_id")),
                "target_name": safe_str(target.get("target_name")),
                # 补充关联字段
                "campaign_id": safe_str(campaign_id),
                "pre_parse_raw_text": json.dumps(target, ensure_ascii=False),  # 原始数据字段
                "etl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 采集时间
            }
            standard_target_list.append(standard_target)

        print(f"  ✅ 活动{campaign_id}获取到{len(standard_target_list)}个目标人群")
        return standard_target_list
    except Exception as e:
        print(f"  ❌ 活动{campaign_id}获取目标人群失败：{str(e)}")
        return []


# ===================== ODPS操作函数 =====================
def init_odps_client():
    """初始化ODPS客户端（删除冗余配置）"""
    try:
        odps = ODPS(project=ODPS_PROJECT)
        print(f"✅ ODPS初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def write_target_data_to_odps(odps, target_data_list):
    """批量写入目标人群数据到ODPS"""
    if not target_data_list:
        print(f"⚠️ 无有效目标人群数据，跳过ODPS写入")
        return

    # 构造写入数据（按表字段顺序）
    write_data = []
    for target in target_data_list:
        row = [
            target["panel_id"],
            target["target_id"],
            target["target_name"],
            target["campaign_id"],
            target["pre_parse_raw_text"],
            target["etl_time"]
        ]
        write_data.append(row)

    try:
        # 校验表是否存在
        if not odps.exist_table(CONFIG["table_name"]):
            raise Exception(f"ODPS表不存在：{CONFIG['table_name']}，请先创建表后再运行脚本")

        table = odps.get_table(CONFIG["table_name"])
        partition_spec = f"dt='{CONFIG['etl_date']}'"

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

        print(f"✅ ODPS写入完成 | 总条数：{len(write_data)} | 分区：{CONFIG['etl_date']}")
    except Exception as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ===================== 主流程 =====================
def main():
    print("=" * 80)
    print("🚀 秒针活动目标人群采集任务启动")
    print(f"📅 采集分区日期：{CONFIG['etl_date']}")
    print(f"📋 目标表名：{CONFIG['table_name']}")
    print("=" * 80)

    try:
        # 1. 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 2. 第一步：获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            raise Exception("未获取到任何campaign_id，任务终止")

        # 3. 第二步：遍历campaign_id，获取目标人群详情
        target_data_list = []
        for campaign_id in campaign_ids:
            # 获取该活动的目标人群列表
            target_list = get_target_detail(token, campaign_id)
            if target_list:
                target_data_list.extend(target_list)
            time.sleep(CONFIG["api"]["interval"])  # 接口间隔

        # 4. 写入ODPS
        write_target_data_to_odps(odps, target_data_list)

        # 5. 任务完成统计
        print("\n" + "=" * 80)
        print("✅ 目标人群采集任务完成！")
        print(f"📊 采集目标人群总数：{len(target_data_list)}条")
        print(f"📊 处理活动数：{len(campaign_ids)}个")
        print(f"📊 数据写入表：{CONFIG['table_name']}（dt={CONFIG['etl_date']}）")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()