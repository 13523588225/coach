# DataWorks 环境下无需配置任何参数，自动获取内置客户端
from odps import ODPS

# 自动读取 DataWorks 内置的 MaxCompute 配置，无需传入 access_id/endpoint
o = ODPS()

# 直接执行操作
print(f"DataWorks 免密连接成功，当前项目：{o.project}")

# 执行 SQL
with o.execute_sql("SELECT count(*) FROM your_table").open_reader() as reader:
    for record in reader:
        print(f"表数据量：{record[0]}")