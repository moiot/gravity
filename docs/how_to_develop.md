# Setup git commit hook

```
cd $working_dir/drc/.git/hooks
ln -s ../../hooks/pre-commit .
```

# How to develop in your local machine

Dependencies like mysql, rocketmq, kafka should be specified using config file.

To help you set it up quickly, we included the mysql dependencies in docker-compose file.


- `cp gravity/configdata/dev.example.toml gravity/configdata/dev.toml`

- Change the configuration in `gravity/configdata/dev.toml`

- `make dev-up` (It will start mysql container on your machine and start gravity)

# How to test it on dev01

**mysql**

host: `mbk-dev01.oceanbase.org.cn`

port: `3306`

user: `root`

**kafka**

host: `mbk-dev01.oceanbase.org.cn`

port: `9092`

**zookeeper**

host: `mbk-dev01.oceanbase.org.cn`

port: `2128`


For example,

To create a topic:

`./kafka-topics.sh --create --topic test --replication-factor 1 --partitions 1 --zookeeper mbk-dev01.oceanbase.org.cn:2181`


To test the producer:

`./kafka-console-producer.sh --topic test --broker-list mbk-dev01.oceanbase.org.cn:9092`

To test the consumer:

`./kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server mbk-dev01.oceanbase.org.cn:9092`


**RockerMQ**
* Admin Tool `https://rocketmq.apache.org/docs/cli-admin-tool/`
* Consumer `https://rocketmq.apache.org/docs/quick-start/`
* Web Monitor `http://mbk-dev01.oceanbase.org.cn:8080/#/`
* Use `rmq-cli` for testing ```make rmq-cli && ./bin/rmq-cli -config=config.toml```

#### Known Unsupported Query
```CREATE TABLE `test_db_create_select`.`test_alike2` select * from test_alike;``` 
[ERROR in query 11] Statement violates GTID consistency: CREATE TABLE ... SELECT.
