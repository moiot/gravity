---
title: Output Configuration
summary: Learn how to configure Output.
---

# Output Configuration

Currently, DRC supports the following Output plugins:

- `async-kafka`: Sends the Input message to Kafka asynchronously.
- `mysql`: Writes data in MySQL.

## `async-kafka` configuration

```toml
#
# The connection configuration of Kafka 
# Required
#
[output]
type = "async-kafka"

[output.config]
output-format = "json" # json or pb
schema-version = "0.1"

[output.config.kafka-global-config]
# required
broker-addrs = ["localhost:9092"]
mode = "async"

# kafka SASL
# optional
[output.config.kafka-global-config.net.sasl]
enable = false
user = ""
password = ""

#
# Kafka topic route
# required
#
[[output.config.routes]]
match-schema = "test"
match-table = "test_table"
dml-topic = "test.test_table"
```

The DML JSON format output by Kafka is as follows:

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

In the above configuration:

- `type` indicates the operation type. The value can be `insert`, `update`, `delete`, and `ddl`.
- `data` indicates the current data of this row.
- `old` indicates the old data of this row (It has a value only when `type` is `update`).

The time field is output in the `rfc3399` format according to the string.

The DDL JSON format output by Kafka is as follows:

```json
{
   "version": "2.0",
   "database":"test",
   "table":"t",
   "type":"ddl",
   "statement": " alter table test.t add column v int"
}
```

## `mysql` configuration

```toml
#
# The connection configuration of the target MySQL cluster
# Required
#
[output]
type = "mysql"

[output.config]
enable-ddl = true # support create & alter table for now. schema and table names will be modified according to routes.

[output.config.target]
host = "127.0.0.1"
username = ""
password = ""
port = 3306

#
# The routing configuration of the target MySQL cluster. "*" is supported for `match-schema` and `match-table`.
# Required
#
[[output.config.routes]]
match-schema = "test"
match-table = "test_source_table_*"
target-schema = "test"
target-table = "test_target_table"

#
# The execution engine configuration of MySQL
# Optional
#
[output.config.execution-engine]
# Whether to enable the write operation of the bidirectional synchronization identifier
use-bidirection = false
```

In the above configuration, if `use-bidirection` is set to "true" as follows, DRC gets the internal identifer of bidirectional synchronization when writing data in the MySQL cluster (by enwrapping the DRC internal table transaction). If `ignore-bidirectional-data` is configured in the source cluster, the write traffic in DRC can be ignored.

```toml
[output.config.execution-engine]
# "true" means enabling the write operation of the bidirectional synchronization identifier.
use-bidirection = true
```
