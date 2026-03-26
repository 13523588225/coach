# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime
import urllib3
from typing import List
from odps import ODPS

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====================== 统一配置中心 + 全局变量 ======================
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
    "table_name": "ods_mz_adm_campaigns_list_api_df",
    "batch_size": 1000
}
# 全局ODPS项目变量（适配通用写入函数）
ODPS_PROJECT = ODPS().project


# ====================== 工具函数 ======================
def safe_str(val):
    """空值/数组/字典安全转换为字符串（适配MaxCompute）"""
    if val is None or val == "" or str(val).lower() == "null":
        return ""
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


# ====================== 通用ODPS写入函数（完全复用你提供的版本） ======================
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


# ====================== 步骤1：获取access_token ======================
def get_access_token():
    """POST请求获取Token（参数从CONFIG读取）"""
    print("🔍 开始获取access_token...")
    try:
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


# ====================== 步骤2：采集活动数据（仅删除请求参数，保留返回字段） ======================
def collect_campaign_data(access_token):
    """GET请求采集活动数据（不传递start_date/end_date请求参数，保留返回字段）"""
    # 核心修改：仅传递access_token，删除所有日期相关请求参数
    request_params = {
        "access_token": access_token
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

        # 过滤有效数据（保留接口返回的所有字段，包括start_date/end_date）
        valid_data = []
        for idx, item in enumerate(raw_data):
            if isinstance(item, dict) and item.get("campaign_id"):
                # 仅补充溯源字段，不修改接口返回的原始字段
                item["pre_parse_raw_text"] = resp.text[:2000]
                item["etl_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                valid_data.append(item)  # 保留活动自身的start_date/end_date
            else:
                print(f"⚠️ 第{idx + 1}条数据无效，跳过")

        print(f"✅ 采集完成 | 总返回{len(raw_data)}条 | 有效{len(valid_data)}条")
        return valid_data
    except Exception as e:
        raise Exception(f"数据采集失败：{str(e)} | 响应：{resp.text[:500]}")


# ====================== 步骤3：组装写入数据（保留start_date/end_date返回字段） ======================
def assemble_odps_data(campaigns):
    """按表结构组装数据（保留接口返回的start_date/end_date字段）"""
    # 核心：保留start_date/end_date字段，对应接口返回的活动自身日期
    field_order = [
        "campaign_id",  # 1. 活动ID
        "start_date",  # 2. 活动自身的开始日期（接口返回）
        "end_date",  # 3. 活动自身的结束日期（接口返回）
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
        # 提取接口返回的start_date/end_date字段，写入MaxCompute
        row = [safe_str(camp.get(field, "")) for field in field_order]
        odps_rows.append(row)

    return odps_rows


# ====================== 主流程 ======================
def main():
    print("=" * 80)
    print("🚀 秒针活动数据采集 → MaxCompute存储（保留返回日期字段版）")
    print("=" * 80)
    try:
        # 1. 获取Token
        access_token = get_access_token()

        # 2. 采集数据（无日期请求参数，保留返回字段）
        campaign_data = collect_campaign_data(access_token)

        # 3. 组装数据（保留start_date/end_date字段）
        odps_rows = assemble_odps_data(campaign_data)

        # 4. 调用通用写入函数写入ODPS
        write_to_odps(
            table_name=CONFIG["table_name"],
            data=odps_rows,
            dt=args['dt']
        )

        print("\n" + "=" * 80)
        print("✅ 全流程执行完成！")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()
