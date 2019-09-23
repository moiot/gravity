# Input Configuration

Currently, Gravity supports the following Input plugins:

- `mysql`: Can be run as three modes: `batch`, `stream`, `replication`
- `mongo`: Can be run as three modes: `batch`, `stream`, `replication`

## `mysql` stream configuration

### MySQL environment configuration

It requirements for the source MySQL database:

- The binlog in the GTID mode is enabled.
- The `_gravity` account is created and the replication related privilege are granted.
- The corresponding tables in the source and target MySQL clusters are created.

MySQL configuration items are as follows:

```
[mysqld]
server_id=4
log_bin=mysql-bin
enforce-gtid-consistency=ON 
gtid-mode=ON
binlog_format=ROW
```

Gravity account privileges are as follows:

```sql
CREATE USER _gravity IDENTIFIED BY 'xxx';
GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT, CREATE, INSERT, UPDATE, DELETE ON *.* TO '_gravity'@'%';
GRANT ALL PRIVILEGES ON _gravity.* TO '_gravity'@'%';
```

### `mysql` stream configuration file

```toml
[input]
type = "mysql"
mode = "stream"

# Whether to ignore the internal data generated in the bidirectional synchronization. "false" by default
[input.config]
ignore-bidirectional-data = false

#
# The connection configuration of the source MySQL cluster
# Required
#
[input.config.source]
host = "127.0.0.1"
username = "_gravity"
password = ""
port = 3306
max-open = 20 # optional, max connections
max-idle = 20 # optional, suggest to be the same as max-open
# The time zone of the source MySQL cluster: https://github.com/go-sql-driver/mysql#loc
location = "Local"

#
# The starting position of incremental synchronization
# Empty by default. Synchronization starts from the current the GTID position.
# Optional
#
[input.config.start-position]
binlog-gtid = "abcd:1-123,egbws:1-234"

#
# The special configuration of the heartbeat detection of the source MySQL cluster. If the heartbeat detection of
# the source MySQL cluster (the write path) is different from [input.mysqlbinlog.source], you can configure this item.
# Not configured by default
# Optional
#
[input.config.source-probe-config]
annotation = "/*some_annotataion*/"
[input.config.source-probe-config.mysql]
host = "127.0.0.1"
username = "_gravity"
password = ""
port = 3306
```

## `mysql` batch configuration

mysql batch mode does not support scanning a table with the following properties:

If a table do not have primary keys, do not have a unique key, and the number of rows exceeds `max-full-dump-count`;

`gravity` will stop and raise error.

You can use `ignore-tables` to ignore these tables. 

```toml
[input]
type = "mysql"
mode = "batch"

#
# The connection configuration of the source MySQL cluster
# Required
#
[input.config.source-master]
host = "127.0.0.1"
username = "_gravity"
password = ""
port = 3306
max-open = 10 # optional, max connections
max-idle = 10 # optional, suggest to be the same as max-open
# The time zone of the source MySQL cluster: https://github.com/go-sql-driver/mysql#loc
location = "Local"

#
# The configuration of the slave for the source MySQL cluster
# If it is specified, the slave is scanned for high priority during the data scanning process.
# Not configured by default
#
[input.config.source-slave]
host = "127.0.0.1"
username = "_gravity"
password = ""
port = 3306
max-open = 100 # optional, max connections
max-idle = 100 # optional, suggest to be the same as max-open
# The time zone of the source MySQL cluster: https://github.com/go-sql-driver/mysql#loc
location = "Local"

#
# The table to be scanned
# Optional. If not specified, output router will be used.
[[input.config.table-configs]]
schema = "test_1"
table = "test_source_*"

[[input.config.table-configs]]
schema = "test_2"
table = "test_source_*"

# Optional
# Enforce the column to scan for tables. If you don't specifiy this value, the system will use a column that has unique index.
# Make sure you know what you are doing: YOU NEED TO SPECIFY A COLUMN THAT HAVE A UNIQUE INDEX.
scan-column = "id"

# you can ignore talbes defined in input.config.table-configs
# gravity do not support scan of these tables if it matches all of these condition:
#
# 1. do not have primary key
# 2. do not have unique key
# 3. the number of rows in this table is greater than max-full-dump-count.
#
# To ignore these tables, you can setup a ingore table list.
[[input.config.ignore-tables]]
schema = "test_1"
table = "test_source_1"

[input.config]

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

### `mysql` replication mode

```toml
[input]
type = "mysql"
mode = "replication"
```

In `replication` mode, it will firs do a `batch` mode table scan, and then start `stream` mode automatically. 

## `mongo` stream configuration

```toml
#
# The connection configuration of the source MongoDB
# Required
#
[input]
type = "mongo"
mode = "stream"

[input.config.source]
host = "127.0.0.1"
port = 27017
username = ""
password = ""

#
# The starting position of the source MongoDB Oplog. If not configured, synchronization starts from the latest Oplog.
# Empty by default
# Optional
#
[input.config]
start-position = 123456

#
# The related configuration of the source MongoDB Oplog concurrency
# "false", "50", "512", and "750ms" respectively by default
# Optional (to be deprecated)
[input.config.gtm-config]
use-buffer-duration = false
buffer-size = 50
channel-size = 512
buffer-duration-ms = "750ms"
```

## `mongo` batch configuration
```toml
[input]
type = "mongo"
mode = "batch"

[input.config]
# how many documents to fetch per query
# Optional
batch-size = 500 

# how many concurrent workers
# Optional
worker-cnt = 10

# how many concurrent requests could be raised to server(for rate limiting)
# Optional
batch-per-second-limit = 1

# if collection has more rows than this threshold, it would be splitted into chunks to scan parallelly
# Optional
chunk-threshold = 500000 

# Required
[input.config.source]
host = "127.0.0.1"
port = 27017
username = ""
password = ""
```

### `mongo` replication mode

```toml
[input]
type = "mongo"
mode = "replication"
```

In `replication` mode, it will firs do a `batch` mode table scan, and then start `stream` mode automatically. 

