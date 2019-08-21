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


#### 编译（可选，可以直接用 docker）

首先，[配置好 Go 语言环境](https://golang.org/doc/install) 并编译


```bash
mkdir -p $GOPATH/src/github.com/moiot/ && cd $GOPATH/src/github.com/moiot/

git clone https://github.com/moiot/gravity.git

cd gravity && make

```


#### MySQL 到 MySQL 同步

创建如下配置文件 `config.toml`

```toml
# name 必填
name = "mysql2mysqlDemo"

# 内部用于保存位点、心跳等事项的库名，默认为 _gravity
internal-db-name = "_gravity"

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

#### MySQL 到 Kafka

创建如下配置文件 `config.toml`

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

## 启动 gravity
从编译完的程序
```bash
bin/gravity -config mysql2mysql.toml
```
从 docker
```bash
docker run -v ${PWD}/config.toml:/etc/gravity/config.toml -d --net=host moiot/gravity:latest
```

## 监控
Gravity 使用 [Prometheus](https://prometheus.io) 和 [Grafana](https://grafana.com/) 实现监控功能。
在运行端口（默认8080）上提供了 Prometheus 标准的指标抓取路径`/metrics`。
在源码路径`deploy/grafana`下提供了 Grafana dashboard 供导入。 