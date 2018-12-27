# 简介

DRC (Data Replication Center) 是包含生产者 (gravity)、消费者、集群管理、消息队列，这 4 个组件的系统。

目前实现了对接 MySQL 的消息生产者。

gravity （生产者）的基本原理是监听 MySQL binlog 事件；然后根据此事件所对应的源端数据库的 schema, 生成标准格式的数据发送到消息队列给消费者使用。


# v0.1 功能列表

### gravity （生产者）

- MySQL binlog 消息发送到 kafka

- 可指定 database, table 与 kafka topic 的对应关系

- 可指定按 Column 组合分配数据到一个 kafka topic 的不同 partition

# gravity 配置参数

使用方需要指定如下的参数:

```toml
# 唯一标识数据源的名字
unique-source-name = "my_unique_name"

# 源端数据库定义
[source]
host = "mbk-dev01.oceanbase.org.cn"
username = "root"
password = ""
port = 3306

output = "kafka"

# kafka broker 地址
[kafka-global]
broker-addrs = ["mbk-dev01.oceanbase.org.cn:9092"]

# kafka partition 数量的定义
[kafka-global.partitions]
test = 1
test16 = 16

# kafka topic 路由规则
[[topic-routes]]
source-db = "test_db_1"
source-table = "table1"
dml-target-topic = "test"
partition-columns = ["id"]

# kafka topic 路由规则
[[topic-routes]]
source-db = "test_db_1"
source-table = "table2"
dml-target-topic = "test16"
partition-columns = ["uid"]
```
主要是需要同步哪个数据的那个表，以及往 kafka 的那个 topic 发送消息。

# v0.1 消息格式定义


## MySQL binglog 订阅

### insert 操作
```json
{
  "version": "0.1",
  "unique_source_name": "Unique Name of the data source",
  "database": "DATABASE_NAME",
  "table": "TABLE_NAME",
  "time_zone": "Time zone of the data source",
  "host": "Host name or IP address of the data source",
  "type": "update",
  "ts": timestampe value of this event,
  "data": {
    "columnA": "valueA",
    "columnB": "valueB"
  },
  "pks": {
     "primaryKeyColumnA": "valueA",
     "primaryKeyColumnB": "valueB"
  }
}
```

### update 操作

```json
{
  "version": "0.1",
  "unique_source_name": "Unique Name of the data source",
  "database": "DATABASE_NAME",
  "table": "TABLE_NAME",
  "time_zone": "Time zone of the data source",
  "host": "Host name or IP address of the data source",
  "type": "update",
  "ts": timestampe value of this event,
  "data": {
    "columnA": "valueA",
    "columnB": "valueB"
  },
  "old": {
    "columnA": "oldValueA"
    "columnB": "valueB"
  },
  "pks": {
      "primaryKeyColumnA": "valueA",
      "primaryKeyColumnB": "valueB"
  }
}
```

## Mongodb oplog 订阅

```json
{
  "version":"0.1",
  "database":"test",
  "collection":"mBKBikeInfo",
  "unique_source_name":"localhost",
  "oplog":{
    "_id":"5a30c55f9ba524e013afb592",
    "operation":"u",
    "namespace":"test.mBKBikeInfo",
    "data":{
      "$set":{"columnB":20}
     },
    "timestamp":1521701954,
    "ordinal":18,
    "source":0,
    "row": {
      "columnA": "valueA"
    }
  }
 }
```

**`oplog.row` 表示整行的数据。由于腾讯云 Mongo oplog 的限制，oplog 中不包含整行的数据，默认情况下 `row` 不存在。`row` 的值是在拿到 oplog 之后向数据源反查得来的，不一定和 oplog 当前的操作对应，延迟大概在 10ms ~ 50 ms （row 比 oplog 更新）**

- `oplog.data.operation` 为 `u`
   update 操作，oplog.row 包含整行数据
   
- `oplog.data.operation` 为 `i`
   insert 操作，oplog.row 不包含数据，从 oplog.data 可以拿到整行数据
   
- `oplog.data.operation` 为 `d`
   delete 操作，oplog.data, oplog.row 都不包含数据，从 oplog._id 可以拿到 _id 值

`oplog.data` 字段表示当前对数据的操作命令。

`oplog.timestamp` 表示此 oplog 操作的时间戳(epoch秒数)。

`oplog.ordinal` 表示此操作在该秒内的序数。

timestamp和ordinal是mongo timestamp拆分出来的，详见https://docs.mongodb.com/manual/reference/bson-types/#timestamps

oplog 实际上是以一个 mongo collection 的形式存放在 mongo 里的，更底层的描述可以参考这里：https://www.kchodorow.com/blog/2010/10/12/replication-internals/