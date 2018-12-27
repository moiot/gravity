当前支持的 Input Plugin 有如下几种

- **mysql** 以 MySQL 作为输入源，支持全量、增量、全量+增量模式
- **mongooplog** 以 MongoDB 的 Oplog 作为输入源，监听 MongoDB 的增量数据变更


### mysql 增量模式

#### MySQL 环境的准备

mysql 对源端 MySQL 的要求如下：
- 开启 gtid 模式的 binlog
- 创建 drc 账户，并赋予 replication 相关权限，以及 drc 数据库的所有权限
- MySQL 源端、目标端相应的表需要创建好

MySQL 配置项如下所示

```
[mysqld]
server_id=4
log_bin=mysql-bin
gtid-mode=ON
binlog_format=ROW
```

drc 账户权限如下所示

```sql
CREATE USER drc IDENTIFIED BY 'xxx';
GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT, INSERT, UPDATE, DELETE ON *.* TO 'drc'@'%';
GRANT ALL PRIVILEGES ON drc.* TO 'drc'@'%';
```

### mysql 增量配置文件
```toml
[input.mysql]
mode = "stream"

# 是否忽略双向同步产生的内部数据，默认值为 false
ignore-bidirectional-data = false

#
# 源端 MySQL 的连接配置
# - 必填
#
[input.mysql.source]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306

#
# 开始增量同步的起始位置。
# - 默认为空，从当前 gtid 位点开始同步
# - 可选
#
[input.mysql.start-position]
binlog-gtid = "abcd:1-123,egbws:1-234"

#
# 源端 MySQL 心跳检测的特殊配置。若源端 MySQL 的心跳检测（写路径）与 [input.mysql.source]
# 不一样的话，可以在此配置。
# - 默认不配置此项。
# - 可选
#
[input.mysql.source-probe-config]
annotation = "/*some_annotataion*/"
[input.mysql.source-probe-config.mysql]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306
```


### mysql 全量模式

```toml
[input.mysql]
mode = "batch"
#
# 源端 MySQL 的连接配置
# - 必填
#
[input.mysql.source-master]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306

#
# 源端 MySQL 从库的配置
# 如果有此配置，则扫描数据时优先从从库扫描
# - 默认不配置此项
#
[input.mysql.source-slave]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306

#
# 需要扫描的表
# - 必填
[[input.mysql.table-configs]]
schema = "test_1"
table = "test_source_*"

[[input.mysql.table-configs]]
schema = "test_2"
table = "test_source_*"

[input.mysql]

# 总体扫描的并发线程数
# - 默认为 10，表示最多允许 10 个表同时扫描
# - 可选
nr-scanner = 10

# 单次扫描所去的行数
# - 默认为 10000，表示一次拉取 10000 行
# - 可选
table-scan-batch = 10000

# 全局限制，每秒所允许的 batch 数
# - 默认为 1
# - 可选
#
batch-per-second-limit = 1
```

对于上面的默认配置，最多允许 10 个并发线程扫描源库，每个线程一次拉取 10000 行；
同时，系统全局每秒扫描 batch 数不超过 1 ，也就是不超过 10000 行每秒。

### mysql 全量+增量
需设置 mysql.mode = "replication"，其余设置分别于全量、增量相同。
系统会先保存起始位点，再执行全量。全量完成后自动从保存的位点开始增量。

### mongooplog

```toml

#
# 源端 Mongo 连接配置
# - 必填
#
[input.mongooplog.source]
host = "127.0.0.1"
port = 27017
username = ""
password = ""

#
# 源端 Mongo Oplog 的起始点，若不配置，则从当前最新的 Oplog 开始同步
# - 默认为空
# - 可选
#
[input.mongooplog]
start-position = 123456

#
# 源端 Mongo Oplog 并发相关配置
# - 默认分别为 false, 50, 512, "750ms"
# - 可选 （准备废弃）
[input.mongooplog.gtm-config]
use-buffer-duration = false
buffer-size = 50
channel-size = 512
buffer-duration-ms = "750ms"
```
