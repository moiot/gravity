---
title: Output Configuration
summary: Learn how to configure Output.
---

# Output Configuration

Currently, Gravity supports the following Output plugins:

- `async-kafka`: Sends the Input message to Kafka asynchronously.
- `mysql`: Writes data in MySQL.
- `elasticsearch`: Stores and index data in Elasticsearch.

## `async-kafka` configuration

For `async-kafka`, Gravity can guarantee data with the same primary key will be delivered to the same
partition sequentially.

If there is a change in the primary key, there is no such guarantee. For example, for a MySQL table
```sql
CREATE TABLE IF NOT EXISTS test (
  id int,
  v int,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

INSERT INTO test (id, v) VALUES (1, 1); // (1, 1)
UPDATE test set v = 2 WHERE id = 1; //(1, 1) --> (1, 2)
UPDATE test set id = 2 WHERE id = 1; // (1, 2) --> (2, 2)

```

There is no guarantee that event `(1, 1) --> (1, 2)` happens before `(1, 2) --> (2, 2)`. These two events may be sent to
different kafka partitions.
 

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
max-open = 20 # optional, max connections
max-idle = 20 # optional, suggest to be the same as max-open

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

In the above configuration, if `use-bidirection` is set to "true" as follows, Gravity gets the internal identifier of bidirectional synchronization when writing data in the MySQL cluster (by wrapping in the Gravity internal table transaction). If `ignore-bidirectional-data` is configured in the source cluster, the write traffic from Gravity can be ignored.

```toml
[output.config.execution-engine]
# "true" means enabling the write operation of the bidirectional synchronization identifier.
use-bidirection = true
```

## `elasticsearch` configuration

Important notices:

- this plugin is still in beta
- it only supports Elasticsearch version 6.x

```
[output]
type = "elasticsearch"

[output.config]
# Whether to ignore 400(bad request) responses.
# Elasticsearch will return 400 when the index name is invalid or the document parsing error.
# it is disabled by default, so the synchronization will be failed when the remote server returned a 400 error.
ignore-bad-request = true

#
# The server configuration of Elasticsearch
# Required
#
[output.config.server]
urls = ["http://127.0.0.1:9200"]
sniff = false
# http timeout, default is 1000ms
timeout = 500

#
# The basic auth configuration
# Optional
#
[output.config.server.auth]
username = ""
password = ""

#
# The routing configuration
# Required
#
[[output.config.routes]]
match-schema = "test"
match-table = "test_source_table_*"
target-index = "test-index" # the default value is the table name of each DML msg
target-type = "_doc" # the default value is 'doc'
# Whether to ignore a DML msg without a primary key.
# output will use the primary key as the id of the document, so the synchronized table must have a primary key.
# it is disabled by default, so the synchronization will be failed when a DML message has no primary key.
ignore-no-primary-key = false
```

### `esmodel` configuration


Important notices：

- this plugin is still in beta
- it supports Elasticsearch version 7.x and 6.x
- it support for dynamically creating indexes
- it supports one-to-one and one-to-many table relationships.
- temporarily does not support many-to-many relationships

configuration demo：
```toml

[output]
type = "esmodel"

[output.config]
# Whether to ignore 400(bad request) responses.
# Elasticsearch will return 400 when the index name is invalid or the document parsing error.
# it is disabled by default, so the synchronization will be failed when the remote server returned a 400 error.
ignore-bad-request = true

#
# The server configuration of Elasticsearch
# Required
#
[output.config.server]
urls = ["http://192.168.1.152:9200"]
sniff = false
# http timeout, default is 1000ms
timeout = 500
# retry count，default 3 
retry-count=3

#
# The basic auth configuration
# Optional
#
[output.config.server.auth]
username = ""
password = ""

# master table strategy
[[output.config.routes]]
match-schema = "test"
# main table
match-table = "student"
# index name
index-name="student_index"
# Type name, 7.x item is invalid
type-name="student"
#shards number
shards-num=1
#replicas number
replicas-num=0
#Included columns, default all
include-column = []
#Excluded column, default empty
exclude-column = []

# columns convert strategy
[output.config.routes.convert-column]
name = "studentName"

# one-to-one table strategy
[[output.config.routes.one-one]]
match-schema = "test"
match-table = "student_detail"
# foreign key column
fk-column = "student_id"
#Included columns, default all
include-column = []
#Excluded column, default empty
exclude-column = []
# Mode, 1: sub-object, 2: index extend
mode = 2
# Property object name, valid when mode is 1.
property-name = "studentDetail"
# Attribute prefix, can be ignored when mode is 1.
property-pre = "sd_"

# columns convert strategy
[output.config.routes.one-one.convert-column]
introduce = "introduceInfo"

# another one-to-one table strategy
[[output.config.routes.one-one]]
match-schema = "test"
match-table = "student_class"
fk-column = "student_id"
include-column = []
exclude-column = []
mode = 1
property-name = "studentClass"
property-pre = ""

[output.config.routes.one-one.convert-column]
name = "className"

# one-to-more table strategy
[[output.config.routes.one-many]]
match-schema = "test"
match-table = "student_parent"
# foreign key column
fk-column = "student_id"
#Included columns, default all
include-column = []
#Excluded column, default empty
exclude-column = []
# Property object name
property-name = "studentParent"

# columns convert strategy
[output.config.routes.one-many.convert-column]
name = "parentName"

```

