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
        "campaign_list_spots_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list_spots",
        "campaign_show_spot_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/show_spot",
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
    # ODPS配置
    "table_name": "ods_mz_adm_cms_show_spot_api_df",  # 最终表名
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


def get_spot_id_str_list(token, campaign_id):
    """从list_spots接口获取指定campaign_id的spot_id_str列表"""
    print(f"  🔍 获取活动{campaign_id}的spot_id列表（/cms/v1/campaigns/list_spots）...")
    try:
        params = {
            "access_token": token,
            "campaign_id": campaign_id
        }
        resp = requests.get(
            CONFIG["api"]["campaign_list_spots_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        raw_data = resp.json()
        spots = raw_data.get("result", {}).get("spots", [])

        # 提取有效spot_id_str
        spot_id_str_list = []
        for spot in spots:
            if isinstance(spot, dict) and spot.get("spot_id_str"):
                spot_id_str_list.append(str(spot.get("spot_id_str")))

        print(f"  ✅ 活动{campaign_id}获取到{len(spot_id_str_list)}个spot_id_str")
        return spot_id_str_list
    except Exception as e:
        print(f"  ❌ 活动{campaign_id}获取spot_id_str失败：{str(e)}")
        return []


def get_spot_detail(token, campaign_id, spot_id_str):
    """调用show_spot接口获取点位详情，返回标准化字段（含pre_parse_raw_text）"""
    print(f"    🔍 获取点位详情 | campaign_id={campaign_id} | spot_id_str={spot_id_str}")
    try:
        params = {
            "access_token": token,
            "campaign_id": campaign_id,
            "spot_id_str": spot_id_str
        }
        resp = requests.get(
            CONFIG["api"]["campaign_show_spot_url"],
            params=params,
            timeout=CONFIG["api"]["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_detail = resp.json().get("result", {})

        # 标准化输出字段（严格匹配指定结构，原始数据字段改为pre_parse_raw_text）
        standard_fields = {
            "publisher_id": safe_str(spot_detail.get("publisher_id")),
            "channel_name": safe_str(spot_detail.get("channel_name")),
            "publisher_name": safe_str(spot_detail.get("publisher_name")),
            "spot_id": safe_str(spot_detail.get("spot_id")),
            "GUID": safe_str(spot_detail.get("GUID")),
            "description": safe_str(spot_detail.get("description")),
            "CAGUID": safe_str(spot_detail.get("CAGUID")),
            "customize": safe_str(spot_detail.get("customize")),
            "report_metrics": safe_str(spot_detail.get("report_metrics")),
            "market": safe_str(spot_detail.get("market")),
            "vending_model": safe_str(spot_detail.get("vending_model")),
            "area_size": safe_str(spot_detail.get("area_size")),
            "linked_siteid": safe_str(spot_detail.get("linked_siteid")),
            "placement_name": safe_str(spot_detail.get("placement_name")),
            "spot_id_str": safe_str(spot_detail.get("spot_id_str")),
            "adposition_type": safe_str(spot_detail.get("adposition_type")),
            # 补充关联字段（原始数据字段命名为pre_parse_raw_text）
            "campaign_id": safe_str(campaign_id),
            "pre_parse_raw_text": json.dumps(spot_detail, ensure_ascii=False),  # 关键修改：字段名变更
            "etl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 采集时间
        }
        return standard_fields
    except Exception as e:
        print(f"    ❌ 点位{spot_id_str}详情获取失败：{str(e)}")
        return None


# ===================== ODPS操作函数 =====================
def init_odps_client():
    """初始化ODPS客户端"""
    try:
        odps = ODPS(project=ODPS_PROJECT)
        options.tunnel.use_instance_tunnel = True
        options.read_timeout = 300
        options.connect_timeout = 60
        options.tunnel.limit_instance_tunnel = False
        print(f"✅ ODPS初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"ODPS初始化失败：{str(e)}")


def create_spot_table(odps):
    """创建点位详情表（原始数据字段改为pre_parse_raw_text）"""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {CONFIG['table_name']} (
        -- 核心输出字段（严格匹配指定结构）
        publisher_id STRING COMMENT '发布商ID',
        channel_name STRING COMMENT '渠道名称(如F18-30 人群包)',
        publisher_name STRING COMMENT '发布商名称(如哔哩哔哩)',
        spot_id STRING COMMENT '点位ID',
        GUID STRING COMMENT 'GUID标识',
        description STRING COMMENT '点位描述',
        CAGUID STRING COMMENT 'CAGUID标识',
        customize STRING COMMENT '自定义字段',
        report_metrics STRING COMMENT '报表指标',
        market STRING COMMENT '市场类型',
        vending_model STRING COMMENT '售卖模式(如常规购买-常规投放)',
        area_size STRING COMMENT '地域范围大小',
        linked_siteid STRING COMMENT '关联站点ID',
        placement_name STRING COMMENT '投放位置名称(如ALL_信息流_KOL3)',
        spot_id_str STRING COMMENT '点位字符串ID(如91m0C)',
        adposition_type STRING COMMENT '广告位置类型(如信息流)',
        -- 关联字段（原始数据字段命名为pre_parse_raw_text）
        campaign_id STRING COMMENT '活动ID',
        pre_parse_raw_text STRING COMMENT '接口原始返回数据(JSON)',  # 关键修改：字段名变更
        etl_time STRING COMMENT '数据采集时间(yyyy-MM-dd HH:mm:ss)'
    ) 
    PARTITIONED BY (dt STRING COMMENT '分区日期(yyyyMMdd)')
    STORED AS ORC
    TBLPROPERTIES (
        'comment' = '秒针活动点位详情表（来源/cms/v1/campaigns/show_spot）',
        'orc.compress' = 'SNAPPY'
    );
    """
    try:
        odps.execute_sql(create_sql)
        print(f"✅ 点位详情表{CONFIG['table_name']}检查/创建成功")
    except Exception as e:
        raise Exception(f"创建ODPS表失败：{str(e)}")


def write_spot_data_to_odps(odps, spot_data_list):
    """批量写入点位详情数据到ODPS（适配pre_parse_raw_text字段）"""
    if not spot_data_list:
        print(f"⚠️ 无有效点位数据，跳过ODPS写入")
        return

    # 构造写入数据（按表字段顺序，包含pre_parse_raw_text）
    write_data = []
    for spot in spot_data_list:
        row = [
            spot["publisher_id"],
            spot["channel_name"],
            spot["publisher_name"],
            spot["spot_id"],
            spot["GUID"],
            spot["description"],
            spot["CAGUID"],
            spot["customize"],
            spot["report_metrics"],
            spot["market"],
            spot["vending_model"],
            spot["area_size"],
            spot["linked_siteid"],
            spot["placement_name"],
            spot["spot_id_str"],
            spot["adposition_type"],
            spot["campaign_id"],
            spot["pre_parse_raw_text"],  # 关键修改：字段名变更
            spot["etl_time"]
        ]
        write_data.append(row)

    try:
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
    print("🚀 秒针活动点位详情采集任务启动")
    print(f"📅 采集分区日期：{CONFIG['etl_date']}")
    print(f"📋 目标表名：{CONFIG['table_name']}")
    print(f"🔧 原始数据字段名：pre_parse_raw_text")
    print("=" * 80)

    try:
        # 1. 初始化
        odps = init_odps_client()
        token = get_miaozhen_token()

        # 2. 创建表（若不存在）
        create_spot_table(odps)

        # 3. 第一步：获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            raise Exception("未获取到任何campaign_id，任务终止")

        # 4. 第二步：遍历campaign_id，获取spot_id_str并调用show_spot
        spot_data_list = []
        for campaign_id in campaign_ids:
            print(f"\n📌 开始处理活动：{campaign_id}")

            # 获取该活动的spot_id_str列表
            spot_id_str_list = get_spot_id_str_list(token, campaign_id)
            if not spot_id_str_list:
                print(f"❌ 活动{campaign_id}无有效spot_id_str，跳过")
                continue

            # 遍历每个spot_id_str，获取点位详情
            for spot_id_str in spot_id_str_list:
                spot_detail = get_spot_detail(token, campaign_id, spot_id_str)
                if spot_detail:
                    spot_data_list.append(spot_detail)
                time.sleep(CONFIG["api"]["interval"])  # 接口间隔

        # 5. 写入ODPS
        write_spot_data_to_odps(odps, spot_data_list)

        # 6. 任务完成统计
        print("\n" + "=" * 80)
        print("✅ 点位详情采集任务完成！")
        print(f"📊 采集点位总数：{len(spot_data_list)}条")
        print(f"📊 处理活动数：{len(campaign_ids)}个")
        print(f"📊 数据写入表：{CONFIG['table_name']}（dt={CONFIG['etl_date']}）")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 任务执行失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()