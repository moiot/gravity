# Input Configuration

Currently, DRC supports the following Input plugins:

- `mysqlbinlog`: Takes the MySQL binlog as the input source and monitors the incremental data mutation of MySQL.
- `mysqlscan`: Scans MySQL tables, which can be used as the full data of MySQL for the usage of Output.
- `mongooplog`: Takes MongoDB Oplog as the input source and monitors the incremental data mutation of MongoDB.

## `mysqlbinlog` configuration

### MySQL environment configuration

The `mysqlbinlog` requirements for the source MySQL database:

- The binlog in the GTID mode is enabled.
- The DRC account is created and the replication related privilege and all the DRC privileges are granted.
- The corresponding tables in the source and target MySQL clusters are created.

MySQL configuration items are as follows:

```
[mysqld]
server_id=4
log_bin=mysql-bin
gtid-mode=ON
binlog_format=ROW
```

Gravity account privileges are as follows:

```sql
CREATE USER drc IDENTIFIED BY 'xxx';
GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT, INSERT, UPDATE, DELETE ON *.* TO 'drc'@'%';
GRANT ALL PRIVILEGES ON drc.* TO 'drc'@'%';
```

### `mysqlbinlog` configuration file

```toml
[input.mysqlbinlog]

# Whether to ignore the internal data generated in the bidirectional synchronization. "false" by default
ignore-bidirectional-data = false

#
# The connection configuration of the source MySQL cluster
# Required
#
[input.mysqlbinlog.source]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306
# The time zone of the source MySQL cluster: https://github.com/go-sql-driver/mysql#loc
location = "Local"

#
# The starting position of incremental synchronization
# Empty by default. Synchronization starts from the current the GTID position.
# Optional
#
[input.mysqlbinlog.start-position]
binlog-gtid = "abcd:1-123,egbws:1-234"

#
# The special configuration of the heartbeat detection of the source MySQL cluster. If the heartbeat detection of
# the source MySQL cluster (the write path) is different from [input.mysqlbinlog.source], you can configure this item.
# Not configured by default
# Optional
#
[input.mysqlbinlog.source-probe-config]
annotation = "/*some_annotataion*/"
[input.mysqlbinlog.source-probe-config.mysql]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306
```

## `mysqlscan` configuration

```toml
[input.mysqlscan]

#
# The connection configuration of the source MySQL cluster
# Required
#
[input.mysqlscan.source-master]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306
# The time zone of the source MySQL cluster: https://github.com/go-sql-driver/mysql#loc
location = "Local"

#
# The configuration of the slave for the source MySQL cluster
# If it is specified, the slave is scanned for high priority during the data scanning process.
# Not configured by default
#
[input.mysqlscan.source-slave]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306
# The time zone of the source MySQL cluster: https://github.com/go-sql-driver/mysql#loc
location = "Local"

#
# The table to be scanned
# Required
[[input.mysqlscan.table-configs]]
schema = "test_1"
table = "test_source_*"

[[input.mysqlscan.table-configs]]
schema = "test_2"
table = "test_source_*"

[input.mysqlscan]

# The number of the concurrent threads for scanning
# "10" by default, which indicates 10 tables are allowed to be scanned at the same time at most.
# Optional
nr-scanner = 10

# The number of rows in a single scan
# "10000" by default, which indicates 10000 rows are pulled at a time
# Optional
table-scan-batch = 10000

# The number of allowed batches per second in global limit
# "1" by default
# Optional
#
batch-per-second-limit = 1

# When neither primary key, nor unique index could be found, we can use full table scan on tables with less rows than `max-full-dump-count`. Otherwise we'll stop and exit.
# - Default 100,000
# - Optional
#
max-full-dump-count = 10000
```

In the above default configuration:

- At most 10 concurrent threads are allowed to scan the source database.
- Each thread pulls 10,000 rows at a time.
- At most one batch (namely 10,000 rows) are allowed to be scanned per second in the global system. 

## `mongooplog` configuration

```toml

#
# The connection configuration of the source MongoDB
# Required
#
[input.mongooplog.source]
host = "127.0.0.1"
port = 27017
username = ""
password = ""

#
# The starting position of the source MongoDB Oplog. If not configured, synchronization starts from the latest Oplog.
# Empty by default
# Optional
#
[input.mongooplog]
start-position = 123456

#
# The related configuration of the source MongoDB Oplog concurrency
# "false", "50", "512", and "750ms" respectively by default
# Optional (to be deprecated)
[input.mongooplog.gtm-config]
use-buffer-duration = false
buffer-size = 50
channel-size = 512
buffer-duration-ms = "750ms"
```