"""
秒针TV监测广告点位详情采集脚本（并行版+ODPS写入）
数据来源：/monitortv/v1/spot/info 接口
依赖spid_str：/monitortv/v1/spot/list 接口
依赖campaign_id：/monitortv/v1/campaigns/list 接口
Token来源：https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get
ODPS写入：清空旧分区+全量写入，保证同一分区数据完整
"""
import requests
import json
import time
import urllib3
from datetime import datetime, date
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 秒针接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "campaign_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/campaigns/list",
    "spot_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/list",
    "spot_info_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/info",
    "auth": {"username": "", "password": ""},  # 动态从数仓获取
    "timeout": 30,
    "request_interval": 0.01,  # 缩短请求间隔为0.01秒
    "max_workers": 10  # 并行线程数（可根据接口限流调整）
}

# 2. ODPS配置（DataWorks自动鉴权）
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {
    "spot_info": "ods_mz_tvm_spot_info_api_df"  # 广告点位详情目标表
}
# 分区日期
PARTITION_DT = args['dt']


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
    """
    从数仓表 ods_mz_user_api_df 查询TVM接口的账号密码
    """
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


# ===================== ODPS写入 =====================
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

    # 清空分区（防重复，保证同一分区数据完整）
    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")
        print(f"✅ 清空分区：{table_name}.{partition_spec}")

    # 写入数据
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入成功：{table_name} | 分区{dt} | 条数{len(data)}")


# ===================== 秒针接口调用 =====================
def get_miaozhen_token() -> str:
    """获取秒针TV监测Token"""
    try:
        # 从数仓获取账号密码并更新配置
        username, password = get_tvm_api_credentials()
        API_CONFIG["auth"]["username"] = username
        API_CONFIG["auth"]["password"] = password

        resp = requests.post(
            API_CONFIG["token_url"],
            data=API_CONFIG["auth"],
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


def get_campaign_ids(token: str) -> List[str]:
    """从campaigns/list接口获取所有campaign_id"""
    try:
        resp = requests.get(
            f"{API_CONFIG['campaign_list_url']}?access_token={token}",
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        campaigns = resp.json().get("result", {}).get("campaigns", [])
        # 提取并去重campaign_id
        campaign_ids = list(set([to_string(c.get("campaign_id")) for c in campaigns if isinstance(c, dict)]))
        print(f"✅ 获取到{len(campaign_ids)}个有效campaign_id")
        return campaign_ids
    except Exception as e:
        raise Exception(f"获取campaign_id失败：{str(e)}")


def get_spid_str_list(token: str, campaign_id: str) -> List[str]:
    """从spot/list接口获取单个campaign_id对应的spid_str列表"""
    try:
        params = {
            "access_token": token,
            "campaign_id": campaign_id
        }
        resp = requests.get(
            API_CONFIG["spot_list_url"],
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_data = resp.json()
        if spot_data.get("error_code") != 0:
            print(f"⚠️ campaign_id={campaign_id} 获取spid_str失败：{spot_data.get('error_message')}")
            return []

        spots = spot_data.get("result", [])
        spid_str_list = [to_string(spot.get("spid_str")) for spot in spots if
                         isinstance(spot, dict) and spot.get("spid_str")]
        print(f"✅ campaign_id={campaign_id} 获取到{len(spid_str_list)}个有效spid_str")
        time.sleep(API_CONFIG["request_interval"])
        return spid_str_list
    except Exception as e:
        print(f"⚠️ campaign_id={campaign_id} 获取spid_str异常：{str(e)}")
        time.sleep(API_CONFIG["request_interval"])
        return []


def get_spot_info_worker(token: str, request_spid_str: str, request_campaign_id: str) -> Optional[Dict]:
    """单个点位详情采集的工作函数（供线程池调用）"""
    try:
        params = {
            "access_token": token,
            "spid_str": request_spid_str,
            "campaign_id": request_campaign_id
        }
        # 构建完整请求URL
        full_request_url = f"{API_CONFIG['spot_info_url']}?access_token={token}&spid_str={request_spid_str}&campaign_id={request_campaign_id}"

        resp = requests.get(
            API_CONFIG["spot_info_url"],
            params=params,
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_info_data = resp.json()

        if spot_info_data.get("error_code") != 0:
            print(f"⚠️ spid_str={request_spid_str} 采集失败：{spot_info_data.get('error_message')}")
            return None

        result = spot_info_data.get("result", {})
        if not isinstance(result, dict):
            print(f"⚠️ spid_str={request_spid_str} 格式异常")
            return None

        etl_datetime = get_etl_datetime()
        # 标准化字段
        standard_spot_info = {
            # 调用参数
            "request_campaign_id": to_string(request_campaign_id),
            "request_spid_str": to_string(request_spid_str),

            # result字段
            "result_spid_str": to_string(result.get("spid_str")),
            "caid": to_string(result.get("caid")),
            "created_time": to_string(result.get("created_time")),
            "placement": to_string(result.get("placement")),
            "channel_name": to_string(result.get("channel_name")),
            "pub_id": to_string(result.get("pub_id")),
            "publisher": to_string(result.get("publisher")),
            "description": to_string(result.get("description")),
            "ad_type": to_string(result.get("ad_type")),
            "category": to_string(result.get("category")),
            "brand": to_string(result.get("brand")),
            "product": to_string(result.get("product")),
            "copyname": to_string(result.get("copyname")),
            "buyingregular": to_string(result.get("buyingregular")),
            "buyingsub": to_string(result.get("buyingsub")),
            "mediasub": to_string(result.get("mediasub")),
            "generalbuying": to_string(result.get("generalbuying")),
            "spottype": to_string(result.get("spottype")),
            "spotplan": to_string(result.get("spotplan")),
            "spot_plan_record_id": to_string(result.get("spot_plan_record_id")),
            "caguid": to_string(result.get("caguid")),
            "celebrity_stids": to_string(result.get("celebrity_stids")),
            "playinfo_stids": to_string(result.get("playinfo_stids")),
            "spots_display_type": to_string(result.get("spots_display_type")),
            "purchasetype": to_string(result.get("purchasetype")),
            "playpurchasetype": to_string(result.get("playpurchasetype")),
            "mm_channe_id": to_string(result.get("mm_channe_id")),
            "play_info": to_string(result.get("play_info")),
            "tag_place": to_string(result.get("tag_place")),
            "multi_tag": to_string(result.get("multi_tag")),

            # 新增字段：完整接口请求URL
            "full_request_url": to_string(full_request_url),

            # 溯源字段
            "pre_parse_raw_text": to_string(json.dumps(result, ensure_ascii=False)),
            "etl_datetime": to_string(etl_datetime)
        }
        print(f"✅ spid_str={request_spid_str} 采集成功")
        time.sleep(API_CONFIG["request_interval"])
        return standard_spot_info
    except Exception as e:
        print(f"⚠️ spid_str={request_spid_str} 采集异常：{str(e)}")
        time.sleep(API_CONFIG["request_interval"])
        return None


# ===================== 主流程 =====================
def main():
    start_time = time.time()
    try:
        # 1. 获取秒针Token
        token = get_miaozhen_token()
        print(f"\n✅ Token获取成功：{token[:10]}...（部分隐藏）")

        # 2. 获取所有campaign_id
        campaign_ids = get_campaign_ids(token)
        if not campaign_ids:
            raise Exception("未获取到任何campaign_id，任务终止")

        # 3. 批量获取所有spid_str
        all_spid_str_with_campaign = []
        for request_campaign_id in campaign_ids:
            spid_str_list = get_spid_str_list(token, request_campaign_id)
            if spid_str_list:
                all_spid_str_with_campaign.extend([(s, request_campaign_id) for s in spid_str_list])

        print(f"\n✅ 累计获取到{len(all_spid_str_with_campaign)}个待采集的(spid_str, campaign_id)组合")
        if not all_spid_str_with_campaign:
            raise Exception("未获取到任何spid_str，任务终止")

        # 4. 多线程并行采集点位详情
        all_spot_info_data = []
        with ThreadPoolExecutor(max_workers=API_CONFIG["max_workers"]) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(get_spot_info_worker, token, spid_str, campaign_id): (spid_str, campaign_id)
                for spid_str, campaign_id in all_spid_str_with_campaign
            }

            # 遍历完成的任务
            for future in as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        all_spot_info_data.append(result)
                except Exception as e:
                    spid_str, campaign_id = future_to_task[future]
                    print(f"⚠️ 任务(spid_str={spid_str}, campaign_id={campaign_id})执行异常：{str(e)}")

        # 5. 格式化写入数据（与表字段顺序严格一致）
        spot_info_write_data = [
            [
                # 调用参数
                t["request_campaign_id"],
                t["request_spid_str"],

                # result所有字段
                t["result_spid_str"],
                t["caid"],
                t["created_time"],
                t["placement"],
                t["channel_name"],
                t["pub_id"],
                t["publisher"],
                t["description"],
                t["ad_type"],
                t["category"],
                t["brand"],
                t["product"],
                t["copyname"],
                t["buyingregular"],
                t["buyingsub"],
                t["mediasub"],
                t["generalbuying"],
                t["spottype"],
                t["spotplan"],
                t["spot_plan_record_id"],
                t["caguid"],
                t["celebrity_stids"],
                t["playinfo_stids"],
                t["spots_display_type"],
                t["purchasetype"],
                t["playpurchasetype"],
                t["mm_channe_id"],
                t["play_info"],
                t["tag_place"],
                t["multi_tag"],

                # 新增字段：完整接口请求URL
                t["full_request_url"],

                # 溯源字段
                t["pre_parse_raw_text"],
                t["etl_datetime"]
            ] for t in all_spot_info_data
        ]

        # 6. 写入ODPS（保证同一分区数据完整）
        write_to_odps(TABLE_NAMES["spot_info"], spot_info_write_data, PARTITION_DT)

        # 7. 打印采集结果
        print(f"\n========== 采集结果汇总 ==========")
        print(f"累计采集到{len(all_spot_info_data)}个广告点位详情数据")
        print(f"任务总耗时：{time.time() - start_time:.2f}秒")

    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()