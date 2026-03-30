"""
秒针TV监测广告点位详情采集脚本
数据来源：ODPS表获取campaign_id + 官方接口采集点位数据
输出：ODPS表 ods_mz_tvm_spot_info_api_df
"""
import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "spot_list_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/list",
    "spot_info_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/info",
    "auth": {"username": "", "password": ""},
    "timeout": 30,
    "request_interval": 0.01,
    "max_workers": 10
}

# ODPS配置
ODPS_PROJECT = ODPS().project
TABLE_NAMES = {"spot_info": "ods_mz_tvm_spot_info_api_df"}
PARTITION_DT = args['dt']


# ===================== 工具函数 =====================
def get_etl_datetime() -> str:
    """获取标准化时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_string(value) -> str:
    """空值安全转换为字符串"""
    return "" if value is None or str(value).lower() == "null" else str(value)


# ===================== 账号认证 =====================
def get_tvm_api_credentials() -> tuple:
    """从数仓查询TVM接口账号密码"""
    o = ODPS(project=ODPS_PROJECT)
    sql = """
          select username, passwords
          from ods_mz_user_api_df
          where api_source = 'TVM' limit 1 \
          """
    with o.execute_sql(sql).open_reader() as reader:
        record = reader[0]
        return record["username"], record["passwords"]


# ===================== ODPS数据写入 =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    """
    写入ODPS表（清空旧分区+全量写入）
    :param table_name: 目标表名
    :param data: 待写入数据列表
    :param dt: 分区日期
    """
    if not data:
        print(f"⚠️ {table_name} 无数据写入")
        return

    o = ODPS(project=ODPS_PROJECT)
    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    # 清空旧分区
    if table.exist_partition(partition_spec):
        o.execute_sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_spec})")

    # 写入新数据
    with table.open_writer(partition=partition_spec, create_partition=True) as writer:
        writer.write(data)
    print(f"✅ 写入成功：{table_name} | 条数{len(data)}")


# ===================== 接口请求核心 =====================
def get_miaozhen_token() -> str:
    """获取接口访问Token"""
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
        raise Exception(f"Token获取失败：{token_data.get('error_message')}")
    return to_string(token_data["result"]["access_token"])


def get_campaign_ids(dt: str) -> List[str]:
    """
    从ODPS表查询指定dt分区的campaign_id
    """
    try:
        o = ODPS(project=ODPS_PROJECT)
        sql = f"""
        SELECT DISTINCT campaign_id 
        FROM ods_mz_tvm_campaigns_list_api_df 
        WHERE dt = '{dt}' AND NVL(campaign_id, '') != ''
        """
        with o.execute_sql(sql).open_reader() as reader:
            campaign_ids = [to_string(record.campaign_id) for record in reader]

        if not campaign_ids:
            raise Exception(f"dt={dt} 无有效campaign_id")
        return campaign_ids

    except errors.ODPSError as e:
        raise Exception(f"ODPS查询campaign_id失败：{str(e)}")


def get_spid_str_list(token: str, campaign_id: str) -> List[str]:
    """获取单个活动下的spid_str列表"""
    try:
        resp = requests.get(
            API_CONFIG["spot_list_url"],
            params={"access_token": token, "campaign_id": campaign_id},
            timeout=API_CONFIG["timeout"],
            verify=False
        )
        resp.raise_for_status()
        spot_data = resp.json()

        if spot_data.get("error_code") != 0:
            return []

        return [to_string(spot["spid_str"]) for spot in spot_data["result"] if spot.get("spid_str")]

    except Exception:
        return []
    finally:
        time.sleep(API_CONFIG["request_interval"])


def get_spot_info_worker(token: str, spid_str: str, campaign_id: str) -> Optional[Dict]:
    """多线程采集单个点位详情数据"""
    try:
        params = {"access_token": token, "spid_str": spid_str, "campaign_id": campaign_id}
        resp = requests.get(API_CONFIG["spot_info_url"], params=params, timeout=API_CONFIG["timeout"], verify=False)
        resp.raise_for_status()
        data = resp.json()

        if data.get("error_code") != 0 or not isinstance(data.get("result"), dict):
            return None

        result = data["result"]
        return {
            "request_campaign_id": campaign_id,
            "request_spid_str": spid_str,
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
            "full_request_url": resp.url,
            "pre_parse_raw_text": json.dumps(result, ensure_ascii=False),
            "etl_datetime": get_etl_datetime()
        }
    except Exception:
        return None
    finally:
        time.sleep(API_CONFIG["request_interval"])


# ===================== 主执行流程 =====================
def main():
    start_time = time.time()
    try:
        # 1. 获取认证Token
        token = get_miaozhen_token()

        # 2. 获取所有活动ID
        campaign_ids = get_campaign_ids(PARTITION_DT)

        # 3. 批量获取点位标识
        task_list = []
        for cid in campaign_ids:
            spid_list = get_spid_str_list(token, cid)
            task_list.extend([(spid, cid) for spid in spid_list])

        if not task_list:
            raise Exception("无有效点位数据")

        # 4. 多线程采集详情
        result_data = []
        with ThreadPoolExecutor(API_CONFIG["max_workers"]) as executor:
            future_map = {
                executor.submit(get_spot_info_worker, token, spid, cid): (spid, cid)
                for spid, cid in task_list
            }

            for future in as_completed(future_map):
                res = future.result()
                if res:
                    result_data.append(res)

        # 5. 数据格式化并写入ODPS
        write_data = [[v for k, v in item.items()] for item in result_data]
        write_to_odps(TABLE_NAMES["spot_info"], write_data, PARTITION_DT)

        # 6. 任务汇总
        print(f"\n任务完成 | 总耗时：{time.time() - start_time:.2f}s | 采集条数：{len(result_data)}")

    except Exception as e:
        print(f"任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()