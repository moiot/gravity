下面以本地 MySQL 实例的同步和数据订阅为例说明 gravity 使用方法。

#### MySQL 环境准备

参考 [MySQL 环境准备](https://github.com/moiot/gravity/blob/master/docs/2.0/03-inputs.md#mysql-%E7%8E%AF%E5%A2%83%E7%9A%84%E5%87%86%E5%A4%87) 准备一下 MySQL 环境。

MySQL 源端和目标端创建需要同步的表

```sql
CREATE TABLE `test`.`test_source_table` (
  `id` int(11),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `test`.`test_target_table` (
  `id` int(11),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```


#### 编译 (TODO: 开源后直接从 github 下载 binary)

首先，[配置好 Go 语言环境](https://golang.org/doc/install) 并编译


```bash
mkdir -p $GOPATH/src/github.com/moiot/ && cd $GOPATH/src/github.com/moiot/

git clone https://github.com/moiot/gravity.git

cd gravity && make

```


#### MySQL 到 MySQL 同步

创建如下配置文件 `mysql2mysql.toml`

```toml
# name 必填
name = "mysql2mysqlDemo"

#
# Input 插件的定义，此处定义使用 mysql
#
[input]
type = "mysql"
mode = "stream"
[input.config.source]
host = "127.0.0.1"
username = "root"
password = ""
port = 3306

#
# Output 插件的定义，此处使用 mysql
#
[output]
type = "mysql"
[output.config.target]
host = "127.0.0.1"
username = "root"
password = ""
port = 3306

# 路由规则的定义
[[output.config.routes]]
match-schema = "test"
match-table = "test_source_table"
target-schema = "test"
target-table = "test_target_table"
```

启动 `gravity`

```bash
bin/gravity -config mysql2mysql.toml
```

`test_source_table` 和 `test_target_table` 之间的同步已经开始，现在可以在源端插入数据，然后在目标端会看到相应的变化。

#### MySQL 到 Kafka

创建如下配置文件 `mysql2kafka.toml`

```toml
name = "mysql2kafkaDemo"

#
# Input 插件的定义，此处定义使用 mysql
#
[input]
type = "mysql"
mode = "stream"
[input.config.source]
host = "127.0.0.1"
username = "root"
password = ""
port = 3306

#
# Output 插件的定义，此处使用 mysql
#
[output]
type = "async-kafka"
[output.config.kafka-global-config]
broker-addrs = ["127.0.0.1:9092"]
mode = "async"

# kafka 路由的定义
[[output.config.routes]]
match-schema = "test"
match-table = "test_source_table"
dml-topic = "test"
```

启动 `gravity`

```bash
bin/gravity -config mysql2kafka.toml
```

MySQL 源库的 `test`.`test_source_table` 这个表的数据变更会发送到 Kafka 的 `test` 这个 topic。