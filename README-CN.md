# 欢迎使用 NebulaGraph Exchange-csv

[English](https://github.com/lkycxb/nebula-exchange-csv/blob/main/README-CN.md)

官网NebulaGraph Exchange 由于开源版不支持导出功能,故特针对开源版实现了nebula-->csv-->nebula的导入导出工具,来完成数据迁移.

## 使用说明

特性 & 注意事项：

*1. 本项目基于NebulaGraph-3.6.1+Spark3.5.0实现.
*2. 依赖相关项目:spark-nebula-connector
*3. 配置文件
    参考:nebula-exchange_spark_3.5/src/test/resources/config
*4. 使用方法
    参考: nebula-exchange_spark_3.5/src/test下com.ubisectech.nebula.exchange.TestExchange的相关测试方法


