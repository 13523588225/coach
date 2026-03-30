import json
import time
import urllib.parse
from datetime import datetime
from typing import Dict, List
import requests
import urllib3
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置（已移除campaign_list_url）
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "spot_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/list",
    "timeout": 30,
    "request_interval": 0.2
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "spot_list": "ods_mz_tvm_spot_list_api_df",
    "campaign_list": "ods_mz_tvm_campaigns_list_api_df"
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


def get_log() -> str:
    """获取日志时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 从MaxCompute查询API账号密码 =====================
def get_tvm_api_credentials():
    o = ODPS(project=ODPS_PROJECT)
    sql = """
    select username, passwords 
    from ods_mz_user_api_df 
    where api_source = 'TVM'
    limit 1
    """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            record = reader[0]
            username = record["username"]
            password = record["passwords"]
            print(f"[{get_log()}] 🔐 成功从数仓获取TVM账号：{username}")
            return username, password
    except errors.ODPSError as e:
        raise Exception(f"❌ 查询账号密码失败：{str(e)}")


# ===================== 从MaxCompute查询campaign_id =====================
def get_campaign_ids(dt: str) -> List[str]:
    """
    从ODPS表查询指定dt分区的campaign_id
    """
    try:
        o = ODPS(project=ODPS_PROJECT)
        sql = f"""
        select distinct campaign_id 
        from {TABLE_NAMES['campaign_list']} 
        where dt = '{dt}'
        """
        with o.execute_sql(sql).open_reader() as reader:
            campaign_ids = []
            for record in reader:
                cid = to_string(record["campaign_id"])
                if cid:
                    campaign_ids.append(cid)
            campaign_ids = list(set(campaign_ids))
        print(f"✅ 从ODPS表获取到{len(campaign_ids)}个有效campaign_id（dt={dt}）")
        return campaign_ids
    except errors.ODPSError as e:
        raise Exception(f"❌ 查询campaign_id失败：{str(e)}")


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取秒针TV监测Token"""
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
        access_token = token_data.get("result", {}).get("access_token")
        if not access_token:
            raise Exception("Token为空")
        return to_string(access_token)
    except Exception as e:
        raise Exception(f"获取Token失败：{str(e)}")


def get_spot_list(token: str, request_campaign_id: str) -> List[Dict]:
    try:
        params = {
            "access_token": token,
            "campaign_id": request_campaign_id
        }
        full_request_url = requests.compat.urljoin(
            API_CONFIG["spot_list_url"],
            f"?{urllib.parse.urlencode(params, safe='=&')}"
        )
        resp = requests.get(
            API_CONFIG["spot_list_url"],
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_list_data = resp.json()

        if spot_list_data.get("error_code") != 0:
            print(f"⚠️ campaign_id={request_campaign_id} 点位列表采集失败：{spot_list_data.get('error_message')}")
            return []

        result = spot_list_data.get("result", [])
        if not isinstance(result, list):
            print(f"⚠️ campaign_id={request_campaign_id} 点位列表格式异常（非数组）")
            return []

        etl_datetime = get_etl_datetime()
        standard_spot_list = []
        for spot in result:
            if not isinstance(spot, dict):
                continue
            standard_spot = {
                "request_campaign_id": to_string(request_campaign_id),
                "spid_str": to_string(spot.get("spid_str")),
                "caid": to_string(spot.get("caid")),
                "created_time": to_string(spot.get("created_time")),
                "placement": to_string(spot.get("placement")),
                "channel_name": to_string(spot.get("channel_name")),
                "pub_id": to_string(spot.get("pub_id")),
                "publisher": to_string(spot.get("publisher")),
                "description": to_string(spot.get("description")),
                "ad_type": to_string(spot.get("ad_type")),
                "category": to_string(spot.get("category")),
                "brand": to_string(spot.get("brand")),
                "product": to_string(spot.get("product")),
                "copyname": to_string(spot.get("copyname")),
                "buyingregular": to_string(spot.get("buyingregular")),
                "buyingsub": to_string(spot.get("buyingsub")),
                "mediasub": to_string(spot.get("mediasub")),
                "generalbuying": to_string(spot.get("generalbuying")),
                "spottype": to_string(spot.get("spottype")),
                "spotplan": to_string(spot.get("spotplan")),
                "spot_plan_record_id": to_string(spot.get("spot_plan_record_id")),
                "caguid": to_string(spot.get("caguid")),
                "celebrity_stids": to_string(spot.get("celebrity_stids")),
                "playinfo_stids": to_string(spot.get("playinfo_stids")),
                "spots_display_type": to_string(spot.get("spots_display_type")),
                "purchasetype": to_string(spot.get("purchasetype")),
                "playpurchasetype": to_string(spot.get("playpurchasetype")),
                "mm_channe_id": to_string(spot.get("mm_channe_id")),
                "play_info": to_string(spot.get("play_info")),
                "tag_place": to_string(spot.get("tag_place")),
                "multi_tag": to_string(spot.get("multi_tag")),
                "full_request_url": to_string(full_request_url),
                "pre_parse_raw_text": to_string(json.dumps(spot, ensure_ascii=False)),
                "etl_datetime": to_string(etl_datetime)
            }
            standard_spot_list.append(standard_spot)

        print(f"✅ campaign_id={request_campaign_id} 采集到{len(standard_spot_list)}个点位列表数据")
        time.sleep(API_CONFIG["request_interval"])
        return standard_spot_list
    except Exception as e:
        print(f"⚠️ campaign_id={request_campaign_id} 点位列表采集异常：{str(e)}")
        time.sleep(API_CONFIG["request_interval"])
        return []


# ===================== ODPS写入 =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    if not data:
        print(f"⚠️ {table_name} 无数据可写入")
        return

    o = ODPS(project=ODPS_PROJECT)
    if not o.exist_table(table_name):
        raise Exception(f"表{table_name}不存在")

    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{table_name}.{partition_spec}")

    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入成功：{table_name} | 分区{dt} | 条数{len(data)}")


# ===================== 主流程 =====================
def main():
    # 核心修改：直接使用DataWorks调度参数 args['dt']，无需传参
    dt = args['dt']
    try:
        # 1. 获取Token
        token = get_miaozhen_token()
        print(f"✅ Token获取成功")

        # 2. 从ODPS表获取campaign_id
        campaign_ids = get_campaign_ids(dt)
        if not campaign_ids:
            raise Exception(f"未获取到任何campaign_id（dt={dt}），任务终止")

        # 3. 遍历采集数据
        all_spot_list_data = []
        for request_campaign_id in campaign_ids:
            spot_list_data = get_spot_list(token, request_campaign_id)
            if spot_list_data:
                all_spot_list_data.extend(spot_list_data)

        print(f"✅ 累计采集到{len(all_spot_list_data)}个广告点位列表数据")

        # 4. 格式化数据
        spot_list_write_data = [
            [
                t["request_campaign_id"],
                t["spid_str"], t["caid"], t["created_time"], t["placement"], t["channel_name"],
                t["pub_id"], t["publisher"], t["description"], t["ad_type"], t["category"],
                t["brand"], t["product"], t["copyname"], t["buyingregular"], t["buyingsub"],
                t["mediasub"], t["generalbuying"], t["spottype"], t["spotplan"], t["spot_plan_record_id"],
                t["caguid"], t["celebrity_stids"], t["playinfo_stids"], t["spots_display_type"],
                t["purchasetype"], t["playpurchasetype"], t["mm_channe_id"], t["play_info"],
                t["tag_place"], t["multi_tag"], t["full_request_url"], t["pre_parse_raw_text"], t["etl_datetime"]
            ] for t in all_spot_list_data
        ]

        # 5. 写入ODPS
        write_to_odps(TABLE_NAMES["spot_list"], spot_list_write_data, dt)

    except Exception as e:
        print(f"❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    # 直接运行主函数，无任何传参
    main()