'''PyODPS 3
请确保不要使用从 MaxCompute下载数据来处理。下载数据操作常包括Table/Instance的open_reader以及 DataFrame的to_pandas方法。
推荐使用 MaxFrame DataFrame（从 MaxCompute 表创建）来处理数据，MaxFrame DataFrame数据计算发生在MaxCompute集群，无需拉数据至本地。
MaxFrame相关介绍及使用可参考：https://help.aliyun.com/zh/maxcompute/user-guide/maxframe
'''
import json
import time
from datetime import datetime
from typing import Dict, List
import requests
import urllib3
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "timeout": 30,
    "request_interval": 0.2,
    "page_size": 50
}

# 2. ODPS配置
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "campaign": "ods_mz_tvm_campaigns_list_api_df"
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
    where api_source = 'TVM' limit 1
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

def get_campaign_list(token: str) -> List[Dict]:
    """分页采集，严格匹配指定字段输出"""
    all_campaigns = []
    page_size = API_CONFIG["page_size"]
    total_page_no = 1
    total_record_no = 0
    etl_datetime = get_etl_datetime()

    try:
        for page_no in range(1, total_page_no + 1):
            full_request_url = (
                f"{API_CONFIG['campaign_list_url']}?access_token={token}"
                f"&pageSize={page_size}&pageNo={page_no}"
            )
            print(f"[{get_log()}] 请求第 {page_no}/{total_page_no} 页")

            resp = requests.get(full_request_url, timeout=API_CONFIG["timeout"], verify=False)
            resp.raise_for_status()
            response_data = resp.json()

            if response_data.get("error_code") != 0:
                raise Exception(f"接口错误：{response_data.get('error_message')}")

            result_data = response_data.get("result", {})
            campaigns = result_data.get("campaigns", [])

            if page_no == 1:
                total_record_no = result_data.get("totalRecordNo", 0)
                total_page_no = result_data.get("totalPageNo", 1)
                print(f"[{get_log()}] 总记录数：{total_record_no} | 总页数：{total_page_no}")

            # 严格按照指定字段名+顺序构建
            for c in campaigns:
                if not isinstance(c, dict):
                    continue
                all_campaigns.append({
                    # 严格匹配用户指定字段顺序+命名
                    "campaign_id": to_string(c.get("campaign_id")),
                    "start_time": to_string(c.get("start_time")),
                    "end_time": to_string(c.get("end_time")),
                    "order_id": to_string(c.get("order_id")),
                    "scheduling": to_string(c.get("scheduling")),
                    "campaign_name": to_string(c.get("campaign_name")),
                    "s_description": to_string(c.get("description")),  # 重命名
                    "created_time": to_string(c.get("created_time")),
                    "advertiser": to_string(c.get("advertiser")),
                    "agency": to_string(c.get("agency")),
                    "brand": to_string(c.get("brand")),
                    "s_status": to_string(c.get("status")),  # 重命名
                    "verify_version": to_string(c.get("verify_version")),
                    "total_net_id": to_string(c.get("total_net_id")),
                    "calculate_type": to_string(c.get("calculate_type")),
                    "totalnet_version": to_string(c.get("totalnet_version")),
                    "sivt_region": to_string(c.get("sivt_region")),
                    "target_list": to_string(c.get("target_list")),
                    "order_title": to_string(c.get("order_title")),
                    "total_record_no": to_string(total_record_no),
                    "total_page_no": to_string(total_page_no),
                    "current_page_no": to_string(page_no),
                    "page_size": to_string(page_size),
                    "full_request_url": to_string(full_request_url),
                    "pre_parse_raw_text": to_string(json.dumps(c, ensure_ascii=False)),
                    "etl_datetime": to_string(etl_datetime)
                })
            time.sleep(API_CONFIG["request_interval"])

        print(f"[{get_log()}] 采集完成，总条数：{len(all_campaigns)}")
        return all_campaigns
    except Exception as e:
        raise Exception(f"分页采集失败：{str(e)}")

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
    print(f"✅ 写入成功：{len(data)}条")

# ===================== 主流程 =====================
def main():
    try:
        token = get_miaozhen_token()
        campaign_data = get_campaign_list(token)

        # 严格按照指定字段顺序写入
        campaign_write_data = [[
            c["campaign_id"],
            c["start_time"],
            c["end_time"],
            c["order_id"],
            c["scheduling"],
            c["campaign_name"],
            c["s_description"],
            c["created_time"],
            c["advertiser"],
            c["agency"],
            c["brand"],
            c["s_status"],
            c["verify_version"],
            c["total_net_id"],
            c["calculate_type"],
            c["totalnet_version"],
            c["sivt_region"],
            c["target_list"],
            c["order_title"],
            c["total_record_no"],
            c["total_page_no"],
            c["current_page_no"],
            c["page_size"],
            c["full_request_url"],
            c["pre_parse_raw_text"],
            c["etl_datetime"]
        ] for c in campaign_data]

        write_to_odps(TABLE_NAMES["campaign"], campaign_write_data, args['dt'])
    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        raise

if __name__ == "__main__":
    main()