import requests
import json
import time
import urllib3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from odps import ODPS, errors

# ===================== 基础配置 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 接口配置
API_CONFIG = {
    "token_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/token/get",
    "spot_info_url": "https://api-tvmonitor.cn.miaozhen.com/monitortv/v1/spot/info",
    "auth": {"username": "", "password": ""},
    "timeout": 30,
    "request_interval": 0.01,
    "max_workers": 50,  # 并发50
    "batch_size": 5000  # 分批入库阈值（满5000条立即写入）
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


def log_cost_time(tag: str, start_time: float, extra_info: str = ""):
    """统一打印耗时日志"""
    cost = round(time.time() - start_time, 2)
    current_time = get_etl_datetime()
    print(f"[{current_time}] ⏱️  {tag} | 耗时：{cost}s {extra_info}")


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


# ===================== ODPS数据写入（分批追加+原子性保证） =====================
def write_to_odps(table_name: str, data: List[List], dt: str):
    """
    写入ODPS表（追加模式）+ 记录写入耗时
    核心保证：批次数据原子性写入（要么全部成功，要么全部失败），避免部分写入
    """
    if not data:
        print(f"⚠️ {table_name} 无数据写入")
        return

    write_start = time.time()
    current_time = get_etl_datetime()
    print(f"\n[{current_time}] 📝 开始写入MaxCompute：表={table_name}，分区dt={dt}，待写入条数={len(data)}")

    o = ODPS(project=ODPS_PROJECT)
    table = o.get_table(table_name)
    partition_spec = f"dt='{dt}'"

    try:
        # 追加写入数据（create_partition=True：分区不存在则自动创建）
        with table.open_writer(partition=partition_spec, create_partition=True) as writer:
            # writer.write是原子操作：要么全部写入成功，要么全部失败
            writer.write(data)

        # 打印写入耗时
        log_cost_time(f"MaxCompute写入完成", write_start, f"表={table_name}，成功写入={len(data)}条")
    except Exception as e:
        # 写入失败直接抛异常，终止任务并提示，避免数据不一致
        raise Exception(f"批次写入失败（条数={len(data)}），错误详情：{str(e)}")


# ===================== 接口请求核心 =====================
def get_miaozhen_token() -> str:
    """获取接口访问Token"""
    start = time.time()
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

    log_cost_time("获取Token", start)
    return to_string(token_data["result"]["access_token"])


def get_campaign_spid_pairs(dt: str) -> List[Tuple[str, str]]:
    """
    从ODPS表查询指定dt分区的campaign_id + spid_str组合
    """
    start = time.time()
    try:
        o = ODPS(project=ODPS_PROJECT)
        sql = f"""
            SELECT request_campaign_id campaign_id, spid_str 
            FROM ods_mz_tvm_spot_list_api_df 
            WHERE dt = '{dt}'
            AND nvl(request_campaign_id, '') <> '' 
            AND nvl(spid_str, '') <> ''
            GROUP BY request_campaign_id, spid_str
        """
        with o.execute_sql(sql).open_reader() as reader:
            pairs = [
                (to_string(record.campaign_id), to_string(record.spid_str))
                for record in reader
            ]

        if not pairs:
            raise Exception(f"dt={dt} 无有效campaign_id + spid_str组合")

        log_cost_time("查询campaign_id+spid_str", start, f"总条数={len(pairs)}")
        return pairs

    except errors.ODPSError as e:
        raise Exception(f"ODPS查询campaign_id+spid_str失败：{str(e)}")


def get_spot_info_worker(token: str, spid_str: str, campaign_id: str) -> Optional[Dict]:
    """多线程采集单个点位详情数据 + 记录单次请求耗时 + 打印error_code错误码"""
    req_start = time.time()
    req_time = get_etl_datetime()

    try:
        params = {"access_token": token, "spid_str": spid_str, "campaign_id": campaign_id}
        resp = requests.get(API_CONFIG["spot_info_url"], params=params, timeout=API_CONFIG["timeout"], verify=False)
        resp.raise_for_status()
        data = resp.json()

        # ===================== 核心新增：获取并打印error_code =====================
        error_code = data.get("error_code")
        error_msg = data.get("error_message", "无错误信息")
        req_cost = round(time.time() - req_start, 2)

        # 成功：error_code=0
        if error_code == 0 and isinstance(data.get("result"), dict):
            print(
                f"[{req_time}] ✅ 请求成功 | campaign_id={campaign_id}, spid_str={spid_str} | error_code={error_code} | 耗时：{req_cost}s")
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
                # 🔥 唯一修改：匹配你的建表字段 s_description
                "s_description": to_string(result.get("description")),
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
        # 业务失败：打印真实error_code
        else:
            print(
                f"[{req_time}] ❌ 业务失败 | campaign_id={campaign_id}, spid_str={spid_str} | error_code={error_code} | 信息：{error_msg} | 耗时：{req_cost}s")
            return None

    # 网络/系统异常：标记error_code=-1
    except Exception as e:
        req_cost = round(time.time() - req_start, 2)
        print(
            f"[{req_time}] ❌ 请求异常 | campaign_id={campaign_id}, spid_str={spid_str} | error_code=-1 | 错误：{str(e)} | 耗时：{req_cost}s")
        return None
    finally:
        time.sleep(API_CONFIG["request_interval"])


# ===================== 主执行流程（分批入库核心逻辑） =====================
def main():
    total_start = time.time()
    BATCH_SIZE = API_CONFIG["batch_size"]
    print(f"\n[{get_etl_datetime()}] 🚀 任务开始 | 并发数：{API_CONFIG['max_workers']} | 分批入库阈值：{BATCH_SIZE}条")

    try:
        # 1. 获取认证Token
        token = get_miaozhen_token()

        # 2. 获取campaign_id + spid_str组合列表
        campaign_spid_pairs = get_campaign_spid_pairs(PARTITION_DT)
        if not campaign_spid_pairs:
            raise Exception("无有效点位组合，任务终止")

        # 3. 提前清空目标分区（保证当天数据是全量的，避免和旧数据混叠）
        o = ODPS(project=ODPS_PROJECT)
        target_table = TABLE_NAMES["spot_info"]
        partition_spec = f"dt='{PARTITION_DT}'"
        table = o.get_table(target_table)
        if table.exist_partition(partition_spec):
            o.execute_sql(f"ALTER TABLE {target_table} DROP PARTITION ({partition_spec})")
            print(f"\n[{get_etl_datetime()}] 🗑️  已清空目标分区：{target_table} dt={PARTITION_DT}")

        # 4. 多线程采集详情 + 分批入库
        print(f"\n[{get_etl_datetime()}] 🔗 开始并发请求接口，总任务数：{len(campaign_spid_pairs)}")
        batch_data = []  # 批次数据缓存（满5000条写入）
        total_success = 0  # 累计成功采集数

        with ThreadPoolExecutor(API_CONFIG["max_workers"]) as executor:
            # 提交所有采集任务
            future_map = {
                executor.submit(get_spot_info_worker, token, spid_str, cid): (spid_str, cid)
                for cid, spid_str in campaign_spid_pairs
            }

            # 遍历完成的任务，分批收集+写入
            for future in as_completed(future_map):
                res = future.result()
                if res:
                    batch_data.append(res)
                    total_success += 1

                    # 满5000条立即写入，保证内存不溢出+及时入库
                    if len(batch_data) >= BATCH_SIZE:
                        # 格式化数据（ODPS写入要求List[List]格式）
                        write_data = [[v for k, v in item.items()] for item in batch_data]
                        # 原子性写入（失败则抛异常终止任务）
                        write_to_odps(target_table, write_data, PARTITION_DT)
                        # 清空批次缓存，准备下一批
                        batch_data = []
                        print(f"\n[{get_etl_datetime()}] 📦 批次入库完成，累计成功采集：{total_success}条")

        # 5. 处理剩余不足5000条的尾部数据
        if batch_data:
            write_data = [[v for k, v in item.items()] for item in batch_data]
            write_to_odps(target_table, write_data, PARTITION_DT)
            print(f"\n[{get_etl_datetime()}] 📦 剩余数据入库完成，本次入库：{len(batch_data)}条")

        # 6. 任务总汇总
        total_cost = round(time.time() - total_start, 2)
        print(f"\n[{get_etl_datetime()}] 🎉 任务全部完成")
        print(f"📊 总耗时：{total_cost}s | 总任务数：{len(campaign_spid_pairs)} | 采集成功：{total_success}条")

    except Exception as e:
        print(f"\n❌ 任务失败：{str(e)}")
        raise


if __name__ == "__main__":
    main()