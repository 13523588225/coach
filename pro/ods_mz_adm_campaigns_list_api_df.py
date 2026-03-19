# -*- coding: utf-8 -*-
import requests
import json
from datetime import datetime, timedelta
import urllib3
from odps import ODPS, options

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====================== 核心配置 ======================
# 1. 接口配置（按你的要求）
TOKEN_URL = "https://api.cn.miaozhen.com/oauth/token?grant_type=password&username=Coach_api&password=Coachapi2026&client_id=COACH2026_API&client_secret=e65798fb-85d6-4c56-aa19-a2435e8fef18"
CAMPAIGN_BASE_URL = "https://api.cn.miaozhen.com/cms/v1/campaigns/list"

# 2. MaxCompute配置（自动获取项目名，无需手动改）
ODPS_TABLE_NAME = "ods_mz_adm_campaigns_list_api_df"  # 你的目标表名
BATCH_SIZE = 1000  # 批量写入大小


# 3. 时间配置
def get_default_dates():
    """默认时间范围：近30天（YYYY-MM-DD）"""
    today = datetime.now()
    end = today.strftime("%Y-%m-%d")
    start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    return start, end


def get_partition_date():
    """分区日期：当天（YYYYMMDD）"""
    return datetime.now().strftime("%Y%m%d")


# ====================== 工具函数 ======================
def safe_str(val):
    """空值/数组/字典安全转换为字符串（适配MaxCompute）"""
    if val is None or val == "" or str(val).lower() == "null":
        return ""
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def init_odps_client():
    """初始化MaxCompute客户端（自动获取项目名）"""
    try:
        odps = ODPS()
        options.tunnel.use_instance_tunnel = True  # 性能优化
        options.read_timeout = 300
        print(f"✅ MaxCompute初始化成功 | 项目：{odps.project}")
        return odps
    except Exception as e:
        raise Exception(f"MaxCompute初始化失败：{str(e)}")


# ====================== 步骤1：获取access_token ======================
def get_access_token():
    """POST请求获取Token（参数拼URL）"""
    print("🔍 开始获取access_token...")
    try:
        resp = requests.post(TOKEN_URL, verify=False, timeout=30)
        resp.raise_for_status()
        token_data = resp.json()
        access_token = token_data["access_token"]
        print(f"✅ Token获取成功：{access_token[:20]}...")
        return access_token
    except Exception as e:
        raise Exception(f"Token获取失败：{str(e)} | 响应：{resp.text[:500]}")


# ====================== 步骤2：采集活动数据 ======================
def collect_campaign_data(access_token):
    """GET请求采集活动数据（access_token拼URL）"""
    start_date, end_date = get_default_dates()
    print(f"\n🔍 采集 {start_date} 至 {end_date} 的活动数据...")

    # 构造完整请求URL（拼接Token+时间参数）
    full_url = (
        f"{CAMPAIGN_BASE_URL}?access_token={access_token}"
        f"&start_date={start_date}&end_date={end_date}"
    )
    print(f"📝 请求URL：{full_url[:100]}...")

    try:
        resp = requests.get(full_url, verify=False, timeout=30)
        resp.raise_for_status()
        raw_data = resp.json()

        # 过滤有效数据（必须是字典+含campaign_id）
        valid_data = []
        for idx, item in enumerate(raw_data):
            if isinstance(item, dict) and item.get("campaign_id"):
                # 补充原始响应文本和采集时间
                item["pre_parse_raw_text"] = resp.text[:2000]  # 截断长文本
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
    """按表结构组装写入数据（严格匹配字段顺序）"""
    # 字段顺序：和你的建表语句完全一致
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
        # 按字段顺序取值，安全转换
        row = [safe_str(camp.get(field, "")) for field in field_order]
        odps_rows.append(row)

    return odps_rows


# ====================== 步骤4：写入MaxCompute ======================
def write_to_odps(odps, odps_rows):
    """写入MaxCompute（含分区管理）"""
    if not odps_rows:
        print("⚠️ 无有效数据，跳过写入")
        return

    partition_dt = get_partition_date()
    partition_spec = f"dt='{partition_dt}'"
    table = odps.get_table(ODPS_TABLE_NAME)

    # 清空当天分区（避免数据重复）
    if table.exist_partition(partition_spec):
        odps.execute_sql(f"ALTER TABLE {ODPS_TABLE_NAME} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{partition_spec}")

    # 批量写入数据
    total = len(odps_rows)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n📤 写入MaxCompute | 总条数：{total} | 分{batches}批写入")

    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        for i in range(batches):
            start = i * BATCH_SIZE
            end = min((i + 1) * BATCH_SIZE, total)
            batch_data = odps_rows[start:end]
            writer.write(batch_data)
            print(f"   第{i + 1}批 | 写入{len(batch_data)}条")

    print(f"✅ 写入完成 | 分区：{partition_dt}")


# ====================== 主流程 ======================
def main():
    print("=" * 80)
    print("🚀 秒针活动数据采集 → MaxCompute存储")
    print("=" * 80)
    try:
        # 1. 初始化MaxCompute
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