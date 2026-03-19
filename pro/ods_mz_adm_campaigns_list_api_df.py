# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import urllib3
from odps import ODPS, options

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====================== 统一配置中心（仅保留核心配置） ======================
CONFIG = {
    "api": {
        "token_url": "https://api.cn.miaozhen.com/oauth/token",
        "campaign_url": "https://api.cn.miaozhen.com/cms/v1/campaigns/list",
        "auth": {
            "grant_type": "password",
            "username": "Coach_api",
            "password": "Coachapi2026",
            "client_id": "COACH2026_API",
            "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
        },
        "timeout": 30,
        "interval": 0.1  # 接口间隔（避免限流）
    },
    "table_name": "ods_mz_adm_basic_show_api_di",
    "batch_size": 1000
}


# ====================== 工具函数 ======================
def safe_str(val):
    """空值/数组/字典安全转换为字符串（适配MaxCompute）"""
    if val is None or val == "" or str(val).lower() == "null":
        return ""
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def get_default_date_range():
    """默认时间范围：近30天（YYYY-MM-DD，适配秒针接口）"""
    today = datetime.now()
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"📅 使用默认时间范围：{start_date} 至 {end_date}")
    return start_date, end_date


def init_odps_client():
    """初始化MaxCompute客户端（DataWorks自动鉴权）"""
    try:
        odps = ODPS(project=ODPS().project)
        print(f"✅ MaxCompute初始化成功 | 目标表：{CONFIG['table_name']}")
        return odps
    except Exception as e:
        raise Exception(f"MaxCompute初始化失败：{str(e)}")


# ====================== 步骤1：获取access_token（基于CONFIG配置） ======================
def get_access_token():
    """POST请求获取Token（参数从CONFIG读取）"""
    print("🔍 开始获取access_token...")
    try:
        # 构造Token请求参数（从CONFIG读取）
        token_params = CONFIG["api"]["auth"]
        resp = requests.post(
            url=CONFIG["api"]["token_url"],
            data=token_params,
            verify=False,
            timeout=CONFIG["api"]["timeout"]
        )
        resp.raise_for_status()
        token_data = resp.json()
        access_token = token_data["access_token"]
        print(f"✅ Token获取成功：{access_token[:20]}...")
        return access_token
    except Exception as e:
        raise Exception(f"Token获取失败：{str(e)} | 响应：{resp.text[:500]}")


# ====================== 步骤2：采集活动数据（默认近30天） ======================
def collect_campaign_data(access_token):
    """GET请求采集活动数据（默认近30天，参数从CONFIG读取）"""
    # 获取默认时间范围
    start_date, end_date = get_default_date_range()

    # 构造完整请求URL（拼接Token+时间参数）
    request_params = {
        "access_token": access_token,
        "start_date": start_date,
        "end_date": end_date
    }
    print(f"📝 请求参数：{json.dumps(request_params, indent=2)}")

    try:
        resp = requests.get(
            url=CONFIG["api"]["campaign_url"],
            params=request_params,
            verify=False,
            timeout=CONFIG["api"]["timeout"]
        )
        resp.raise_for_status()
        raw_data = resp.json()

        # 过滤有效数据
        valid_data = []
        for idx, item in enumerate(raw_data):
            if isinstance(item, dict) and item.get("campaign_id"):
                # 补充溯源字段
                item["pre_parse_raw_text"] = resp.text[:2000]
                item["etl_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                valid_data.append(item)
            else:
                print(f"⚠️ 第{idx + 1}条数据无效，跳过")

        print(f"✅ 采集完成 | 总返回{len(raw_data)}条 | 有效{len(valid_data)}条")
        return valid_data
    except Exception as e:
        raise Exception(f"数据采集失败：{str(e)} | 响应：{resp.text[:500]}")


# ====================== 步骤3：组装MaxCompute写入数据 ======================
def assemble_odps_data(campaigns):
    """按表结构组装数据（仅保留基础活动字段+溯源字段）"""
    # 字段顺序：仅保留核心基础字段
    field_order = [
        "campaign_id",  # 1. 活动ID
        "start_date",  # 2. 开始日期
        "end_date",  # 3. 结束日期
        "advertiser_name",  # 4. 广告主名称
        "agency_name",  # 5. 代理商名称
        "brand_name",  # 6. 品牌名称
        "calculation_type",  # 7. 计算类型
        "campaign_name",  # 8. 活动名称
        "campaign_type",  # 9. 活动类型
        "creator_name",  # 10. 创建人
        "description",  # 11. 活动描述
        "linked_iplib",  # 12. 关联IP库
        "linked_panels",  # 13. 关联面板
        "linked_siteids",  # 14. 关联站点ID
        "slot_type",  # 15. 广告位类型
        "pre_parse_raw_text",  # 16. 源解析文本
        "etl_datetime"  # 17. 数据落地时间
    ]

    odps_rows = []
    for camp in campaigns:
        row = [safe_str(camp.get(field, "")) for field in field_order]
        odps_rows.append(row)

    return odps_rows


# ====================== 步骤4：写入MaxCompute（基于CONFIG配置） ======================
def write_to_odps(odps, odps_rows):
    """写入MaxCompute（参数从CONFIG读取）"""
    if not odps_rows:
        print("⚠️ 无有效数据，跳过写入")
        return

    table_name = CONFIG["table_name"]
    batch_size = CONFIG["batch_size"]
    partition_dt = datetime.now().strftime("%Y%m%d")
    partition_spec = f"dt='{partition_dt}'"

    # 校验表是否存在
    if not odps.exist_table(table_name):
        raise Exception(f"MaxCompute表不存在：{table_name}，请先执行建表语句")

    table = odps.get_table(table_name)

    # 清空当天分区
    if table.exist_partition(partition_spec):
        drop_sql = f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})"
        odps.execute_sql(drop_sql)
        print(f"✅ 清空分区：{partition_spec}")

    # 批量写入（使用CONFIG的batch_size）
    total = len(odps_rows)
    batches = (total + batch_size - 1) // batch_size
    print(f"\n📤 写入MaxCompute | 表：{table_name} | 总条数：{total} | 分{batches}批写入")

    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        for i in range(batches):
            start = i * batch_size
            end = min((i + 1) * batch_size, total)
            batch_data = odps_rows[start:end]
            writer.write(batch_data)
            print(f"   第{i + 1}批 | 写入{len(batch_data)}条")

    # 验证写入结果
    count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE dt='{partition_dt}'"
    count = odps.execute_sql(count_sql).open_reader().read()[0][0]
    print(f"✅ 写入验证 | {table_name} 分区{partition_dt}共{count}条数据")


# ====================== 主流程（核心功能） ======================
def main():
    print("=" * 80)
    print("🚀 秒针活动数据采集 → MaxCompute存储（最终适配版）")
    print("=" * 80)
    try:
        # 1. 初始化MaxCompute（使用你指定的逻辑）
        odps = init_odps_client()

        # 2. 获取Token
        access_token = get_access_token()

        # 3. 采集数据
        campaign_data = collect_campaign_data(access_token)

        # 4. 组装数据
        odps_rows = assemble_odps_data(campaign_data)

        # 5. 写入MaxCompute
        write_to_odps(odps, odps_rows)

        print("\n" + "=" * 80)
        print("✅ 全流程执行完成！")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()