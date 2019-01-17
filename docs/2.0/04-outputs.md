当前支持的 Output Plugin 有如下几种

- **async-kafka** 以异步方式向 Kafka 发送 Input 的消息
- **mysql** 写 MySQL

下面依次解释各个 Plugin 的配置选项

### async-kafka

```toml
#
# 目标端 Kafka 连接配置
# - 必填
#
[output]
type = "async-kafka"

#
# 目标端编码规则：输出类型和版本号
# - 可选
[output.config]
# 默认为 json
output-format = "json"
# 默认为 0.1 版本
schema-version = "0.1"

[output.config.kafka-global-config]
# - 必填
broker-addrs = ["localhost:9092"]
mode = "async"

# 目标端 kafka SASL 配置
# - 可选
[output.config.kafka-global-config.net.sasl]
enable = false
user = ""
password = ""

#
# 目标端 Kafka 路由配置
# - 必填
#
[[output.config.routes]]
match-schema = "test"
match-table = "test_table"
dml-topic = "test.test_table"
```

Kafka 输出的 DML json 格式如下

```json
{
   "version": "2.0",
   "database":"test",
   "table":"e",
   "type":"update",
   "data":{
      "id":1,
      "m":5.444,
      "c":"2016-10-21 05:33:54.631000+08:00",
      "comment":"I am a creature of light."
   },
   "old":{
      "m":4.2341,
      "c":"2016-10-21 05:33:37.523000"
   }
}
```

其中：
`type` 表示操作类型: `insert`, `update`, `delete`, `ddl`；
`data` 表示当前行此时的数据；
`old` 表示当前行之前的数据（仅在 `update` 时有值）

时间类型的字段采用 `rfc3399` 的格式按照字符串输出。

Kafka 输出的 DDL json 格式如下
```json
{
   "version": "2.0",
   "database":"test",
   "table":"t",
   "type":"ddl",
   "statement": " alter table test.t add column v int"
}
```


### mysql

```toml
#
# 目标端 MySQL 连接配置
# - 必填
#
[output]
type = "mysql"

[output.config]
enable-ddl = true # 当前支持 create & alter table 语句。库表名会根据路由信息调整。

[output.config.target]
host = "127.0.0.1"
username = ""
password = ""
port = 3306

#
# 目标端 MySQL 路由配置；match-schema, match-table 支持 * 匹配
# - 必填
#
[[output.config.routes]]
match-schema = "test"
match-table = "test_source_table_*"
target-schema = "test"
target-table = "test_target_table"

#
# MySQL 执行引擎配置
# - 可选
#
[output.config.execution-engine]
# 开启双向同步标识的写入
use-bidirection = false
```


在上述配置中，如果配置了

```toml
[output.config.execution-engine]
# 开启双向同步标识的写入
use-bidirection = true
```

DRC 在写入目标端 MySQL 的时会打上双向同步的内部标识（通过封装 drc 内部表事务的方式），在源端配置好 `ignore-bidirectional-data` 就可以忽略 DRC 内部的写流量。