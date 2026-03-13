'''PyODPS 3
请确保不要使用从 MaxCompute下载数据来处理。下载数据操作常包括Table/Instance的open_reader以及 DataFrame的to_pandas方法。
推荐使用 MaxFrame DataFrame（从 MaxCompute 表创建）来处理数据，MaxFrame DataFrame数据计算发生在MaxCompute集群，无需拉数据至本地。
MaxFrame相关介绍及使用可参考：https://help.aliyun.com/zh/maxcompute/user-guide/maxframe
'''

import requests
import pandas as pd
from odps import ODPS
from odps import options
import json
from datetime import datetime

# ===================== 1. 配置参数（根据你的DataWorks环境修改） =====================
# 接口请求配置
API_URL = "https://api.cn.miaozhen.com/oauth/token"
API_PARAMS = {
    "grant_type": "password",
    "username": "Coach_api",
    "password": "Coachapi2026",
    "client_id": "COACH2026_API",
    "client_secret": "e65798fb-85d6-4c56-aa19-a2435e8fef18"
}

# DataWorks/ODPS 配置（DataWorks内置环境可直接用默认配置，无需改ak/sk）
ODPS_PROJECT = "coach_marketing_hub"  # 替换为你的DataWorks项目名
ODPS_TABLE = "ods_api_demo"  # 替换为要写入的表名
# 若需指定ODPS endpoint（非必须，DataWorks内置环境自动识别）
# options.end_point = "http://service.cn.maxcompute.aliyun.com/api"


# ===================== 2. 核心函数：读取接口数据 =====================
def fetch_api_data():
    """
    调用目标接口，获取并返回格式化的字典数据
    """
    try:
        # 发送GET请求（接口参数通过URL拼接，也可改用params参数更规范）
        response = requests.get(
            API_URL,
            params=API_PARAMS,
            timeout=30,  # 超时时间30秒
            verify=False  # 忽略SSL证书验证（DataWorks内网环境常用）
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
    注意：需提前在DataWorks中创建对应结构的表（示例表结构见注释）
    """
    # 初始化ODPS客户端（DataWorks内置环境自动获取身份凭证，无需配置ak/sk）
    o = ODPS(
        project=ODPS_PROJECT,
        # DataWorks内置环境无需填写access_id/access_key，自动使用项目角色
    )

    # 数据格式化（适配数据表结构，示例：新增写入时间、扁平化数据）
    write_data = {
        "access_token": data.get("access_token", ""),
        "write_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 写入时间戳
    }

    # 写入数据表（支持覆盖/追加，此处用追加模式）
    try:
        # 获取数据表对象
        table = o.get_table(ODPS_TABLE)
        # 开启写入会话
        with table.open_writer(partition=None, create_partition=True) as writer:
            # 按表字段顺序构造数据行（需与你的表结构完全匹配）
            row = [
                write_data["access_token"],
                write_data["write_time"]
            ]
            writer.write([row])
        print(f"数据成功写入DataWorks表 {ODPS_TABLE}")

    except Exception as e:
        raise Exception(f"写入DataWorks失败：{str(e)}")


# ===================== 4. 主执行流程 =====================
if __name__ == "__main__":
    try:
        # 步骤1：读取接口数据
        api_data = fetch_api_data()

        # 步骤2：写入DataWorks
        write_to_dataworks(api_data)

        print("全流程执行完成！")

    except Exception as e:
        # 捕获所有异常并输出（DataWorks中异常会触发任务失败）
        print(f"任务执行失败：{str(e)}")
        raise  # 抛出异常，让DataWorks任务标记为失败
