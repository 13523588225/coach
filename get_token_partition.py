import requests
from odps import ODPS
from odps import options
import json
from datetime import datetime

# ===================== 1. 配置参数（已填充你的项目信息） =====================
# 接口请求配置
API_URL = "https://api.cn.miaozhen.com/oauth/token"
API_PARAMS = {
    "grant_type": "password",
    "username": "Coach_api",
    "password": "Coachapi2026",
    "client_id": "COACH2026_API",
    "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
}

# DataWorks/ODPS 配置
ODPS_PROJECT = ODPS().project
ODPS_TABLE = "ods_api_demo"
# 新增：分区配置（按日期分区，格式：dt=yyyy-mm-dd）
PARTITION_KEY = "dt"  # 分区字段名（需与表结构一致）
# 分区值：默认取当前日期，也可根据需求改为指定日期
PARTITION_VALUE = datetime.now().strftime("%Y-%m-%d")
PARTITION_SPEC = f"{PARTITION_KEY}='{PARTITION_VALUE}'"  # 分区规范格式

print(f"当前项目{ODPS().project}")

# 开启MaxFrame优化（符合官方规范，避免本地下载数据）
options.sql.use_maxframe = True


# ===================== 2. 核心函数：读取接口数据（POST请求） =====================
def fetch_api_data():
    """
    调用目标接口（POST请求），获取并返回格式化的字典数据
    """
    try:
        # 发送POST请求
        response = requests.post(
            API_URL,
            data=API_PARAMS,
            timeout=30,
            verify=False,
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        # 校验请求是否成功
        response.raise_for_status()
        # 解析JSON数据
        data = response.json()
        print(f"接口返回数据：{json.dumps(data, ensure_ascii=False, indent=2)}")
        return data

    except requests.exceptions.Timeout:
        raise Exception("接口请求超时，请检查网络或接口可用性")
    except requests.exceptions.HTTPError as e:
        raise Exception(f"接口请求失败，HTTP状态码：{response.status_code}，详情：{e}")
    except json.JSONDecodeError:
        raise Exception("接口返回非JSON格式数据，无法解析")
    except Exception as e:
        raise Exception(f"接口数据读取异常：{str(e)}")


# ===================== 3. 核心函数：写入DataWorks（MaxCompute，支持分区） =====================
def write_to_dataworks(data):
    """
    将接口数据写入DataWorks分区表
    表结构要求：access_token (STRING)、write_time (DATETIME)、dt (STRING)（分区字段）
    """
    # 初始化ODPS客户端（DataWorks内置环境自动鉴权）
    o = ODPS(project=ODPS_PROJECT)

    # 数据格式化（包含分区字段）
    write_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_data = {
        "access_token": data.get("access_token", ""),
        "write_time": write_time,
        PARTITION_KEY: PARTITION_VALUE  # 分区字段值
    }

    try:
        # 校验表是否存在
        if not o.exist_table(ODPS_TABLE):
            raise Exception(f"数据表 {ODPS_TABLE} 不存在，请先创建")

        # 清空指定分区（替代整表TRUNCATE，避免删除其他分区数据）
        truncate_sql = f"TRUNCATE TABLE {ODPS_TABLE} PARTITION ({PARTITION_SPEC})"
        o.execute_sql(truncate_sql)
        print(f"已清空分区 {PARTITION_SPEC}")

        # 写入数据到指定分区（自动创建分区，若不存在）
        table = o.get_table(ODPS_TABLE)
        with table.open_writer(
                partition=PARTITION_SPEC,  # 指定分区
                create_partition=True  # 分区不存在时自动创建
        ) as writer:
            # 行数据顺序需与表字段顺序一致（access_token, write_time, dt）
            row = [write_data["access_token"], write_data["write_time"], write_data[PARTITION_KEY]]
            writer.write([row])
        print(f"数据成功写入表 {ODPS_TABLE} 的分区 {PARTITION_SPEC}")

    except Exception as e:
        raise Exception(f"写入DataWorks失败：{str(e)}")


# ===================== 4. 主执行流程 =====================
if __name__ == "__main__":
    try:
        # 步骤1：POST请求读取接口数据
        api_data = fetch_api_data()

        # 步骤2：写入DataWorks分区表
        write_to_dataworks(api_data)

        print("全流程执行完成！")

    except Exception as e:
        print(f"任务执行失败：{str(e)}")
        raise  # 触发DataWorks任务失败，便于监控