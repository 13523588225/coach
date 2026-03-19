# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime
import urllib3
from odps import ODPS, options

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置项
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
    },
    "table_name": "ods_mz_adm_campaigns_list_api_df",
}


# ====================== 核心工具函数 ======================
def safe_str(val):
    """
    安全转换字符串：
    1. 空值/Null转为空字符串
    2. 数组/字典序列化为JSON字符串（适配linked_panels/linked_siteids）
    """
    if val is None or val == "" or str(val).lower() == "null":
        return ""
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def get_etl_datetime():
    """获取数据落地时间（格式：YYYY-MM-DD HH:MM:SS）"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ====================== 获取秒针Access Token ======================
def get_miaozhen_token():
    """获取秒针接口Token（标准Bearer方式）"""
    try:
        resp = requests.post(
            CONFIG["api"]["token_url"],
            data=CONFIG["api"]["auth"],
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
            timeout=CONFIG["api"]["timeout"]
        )
        resp.raise_for_status()  # 触发HTTP错误（如401/500）
        token_data = resp.json()
        token = token_data["access_token"]  # 直接取值（Token必返，无则抛错）
        print("✅ Token获取成功")
        return token
    except Exception as e:
        raise Exception(f"Token获取失败：{str(e)}")


# ====================== 采集活动列表数据（适配数组返回格式） ======================
def collect_campaign_data(token):
    """采集活动列表（接口返回数组，无任何dict.get()调用）"""
    try:
        # Token放在请求头（秒针官方标准方式）
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.get(
            CONFIG["api"]["campaign_url"],
            headers=headers,
            verify=False,
            timeout=CONFIG["api"]["timeout"]
        )
        resp.raise_for_status()

        # 接口返回数组，直接遍历（核心修复：无.get()）
        raw_data = resp.json()
        raw_text = resp.text  # 保存原始响应文本用于溯源
        valid_campaigns = []

        # 过滤有效数据（必须是字典+含campaign_id）
        for idx, item in enumerate(raw_data):
            if isinstance(item, dict) and item.get("campaign_id"):
                item["pre_parse_raw_text"] = raw_text  # 追加原始文本
                valid_campaigns.append(item)
            else:
                print(f"⚠️ 第{idx + 1}条数据无效（非字典/无campaign_id），跳过")

        print(f"✅ 采集完成 | 总返回{len(raw_data)}条 | 有效{len(valid_campaigns)}条")
        return valid_campaigns
    except Exception as e:
        raise Exception(f"活动采集失败：{str(e)}")


# ====================== 组装ODPS数据（严格匹配表字段顺序） ======================
def assemble_odps_data(campaigns):
    """按指定表结构字段顺序组装数据"""
    etl_time = get_etl_datetime()
    odps_rows = []

    # 严格对应表结构的字段顺序（核心：和建表语句一一对应）
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
        "linked_panels",  # 13. 关联面板（数组转JSON）
        "linked_siteids",  # 14. 关联站点ID（数组转JSON）
        "slot_type",  # 15. 广告位类型
        "pre_parse_raw_text",  # 16. 源解析文本
        "etl_datetime"  # 17. 数据落地时间
    ]

    for camp in campaigns:
        # 按字段顺序取值，安全转换
        row = [
            safe_str(camp.get(field)) for field in field_order[:-1]  # 前16个字段
        ]
        row.append(etl_time)  # 最后追加落地时间
        odps_rows.append(row)

    return odps_rows


# ====================== 写入ODPS表 ======================
def write_to_odps(odps_rows):
    """写入ODPS表（自动处理分区）"""
    if not odps_rows:
        print("⚠️ 无有效数据，跳过写入")
        return

    try:
        # 初始化ODPS客户端（自动获取项目名）
        odps = ODPS()
        project_name = odps.project
        print(f"✅ ODPS初始化成功 | 项目：{project_name}")

        # 校验表是否存在
        table_name = CONFIG["table_name"]
        if not odps.exist_table(table_name):
            raise Exception(f"表{table_name}不存在，请先执行建表语句")

        # 分区日期（当天）
        partition_dt = '20260318'
        partition_spec = f"dt='{partition_dt}'"

        # 清空分区（避免数据重复）
        table = odps.get_table(table_name)
        if table.exist_partition(partition_spec):
            odps.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
            print(f"✅ 清空分区：{partition_spec}")

        # 写入数据
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            writer.write(odps_rows)

        print(f"✅ 数据写入完成 | 分区：{partition_dt} | 条数：{len(odps_rows)}")
    except Exception as e:
        raise Exception(f"ODPS写入失败：{str(e)}")


# ====================== 主流程 ======================
def main():
    print("=" * 80)
    print("🚀 秒针活动列表采集任务启动")
    print("=" * 80)
    try:
        # 1. 获取Token
        token = get_miaozhen_token()

        # 2. 采集活动数据
        campaigns = collect_campaign_data(token)
        if not campaigns:
            print("⚠️ 无有效活动数据，任务终止")
            return

        # 3. 组装ODPS数据（严格匹配字段顺序）
        odps_rows = assemble_odps_data(campaigns)

        # 4. 写入ODPS
        write_to_odps(odps_rows)

        print("\n" + "=" * 80)
        print("✅ 任务执行完成！")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise  # 抛出异常，让DataWorks捕获失败


if __name__ == "__main__":
    main()