
# Gravity
------------------------------------

[![Build Status](https://travis-ci.org/moiot/gravity.svg?branch=master)](https://travis-ci.org/moiot/gravity.svg?branch=master)

![2.0 Product](docs/2.0/product.png)


Gravity 是一款数据复制组件，提供全量、增量数据同步，以及向消息队列发布数据更新。

DRC 的设计目标是：
- 支持多种数据源和目标的，可灵活定制的数据复制组件
- 支持基于 Kubernetes 的 PaaS 平台，简化运维任务


### 使用场景

- 大数据总线：发送 MySQL Binlog，Mongo Oplog 的数据变更到 kafka 供下游消费
- 单向数据同步：MySQL --> MySQL 的全量、增量同步
- 双向数据同步：MySQL <--> MySQL 的双向增量同步，同步过程中可以防止循环复制
- 分库分表到合库的同步：MySQL 分库分表 --> 合库的同步，可以指定源表和目标表的对应关系
- 在线数据变换：同步过程中，可支持对进行数据变换

### 功能列表

- 数据源

|   | 是否支持  |
|---|---|
|  MySQL Binlog | ✅  | 
|  MySQL 全量 |  ✅ |   
|  Mongo Oplog | ✅  | 
|  TiDB Binlog | 开发中  |
|  PostgreSQL WAL | 开发中  |

- 数据输出

|   | 是否支持  |
|---|---|
| Kafka | ✅  | 
|  MySQL/TiDB |  ✅ |   
|  Mongo DB | 开发中  | 


- 数据变换

|   | 是否支持  |
|---|---|
| 数据过滤 | ✅  | 
|  重命令列 |  ✅ |   
|   删除列|✅| 


### 文档

[架构简介](docs/2.0/00-arch.md)

[快速上手](docs/2.0/01-quick-start.md)

[配置手册](docs/2.0/02-config-index.md)

[集群部署](https://github.com/moiot/gravity-operator)
