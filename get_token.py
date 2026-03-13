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
ODPS_PROJECT = "coach_marketing_hub"
ODPS_TABLE = "ods_api_demo"
# options.end_point = "http://service.cn.maxcompute.aliyun.com/api"
# 开启MaxFrame优化（符合官方规范，避免本地下载数据）
# options.sql.use_maxframe = True

# ===================== 2. 核心函数：读取接口数据（改为POST请求） =====================
def fetch_api_data():
    """
    调用目标接口（POST请求），获取并返回格式化的字典数据
    """
    try:
        # 发送POST请求（核心修改：从GET改为POST，参数通过data传递）
        response = requests.post(
            API_URL,
            data=API_PARAMS,  # POST请求参数放在data中（表单格式）
            timeout=30,
            verify=False,
            headers={
                "Content-Type": "application/x-www-form-urlencoded"  # 适配OAuth2.0标准请求头
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

# ===================== 3. 核心函数：写入DataWorks（MaxCompute） =====================
def write_to_dataworks(data):
    """
    将接口数据写入DataWorks数据表
    表结构要求：access_token (STRING)、write_time (DATETIME)
    """
    # 初始化ODPS客户端（DataWorks内置环境自动鉴权）
    o = ODPS(project=ODPS_PROJECT)

    # 数据格式化（严格匹配表字段）
    write_data = {
        "access_token": data.get("access_token", ""),
        "write_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        # 校验表是否存在
        if not o.exist_table(ODPS_TABLE):
            raise Exception(f"数据表 {ODPS_TABLE} 不存在，请先创建")
        
        # 写入数据（追加模式，无分区）
        table = o.get_table(ODPS_TABLE)
        with table.open_writer(partition=None, create_partition=False) as writer:
            row = [write_data["access_token"], write_data["write_time"]]
            writer.write([row])
        print(f"数据成功写入表 {ODPS_TABLE}")

    except Exception as e:
        raise Exception(f"写入DataWorks失败：{str(e)}")

# ===================== 4. 主执行流程 =====================
if __name__ == "__main__":
    try:
        # 步骤1：POST请求读取接口数据
        api_data = fetch_api_data()
        
        # 步骤2：写入DataWorks
        # write_to_dataworks(api_data)
        
        print("全流程执行完成！")

    except Exception as e:
        print(f"任务执行失败：{str(e)}")
        raise  # 触发DataWorks任务失败，便于监控