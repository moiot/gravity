# Overview
## Basics
explain what is drc and what can drc do for you
## Gollasry
# Get drc
## Get package
TODO: we can add a command "make package" to generate a drc package, include: bin, conf, log etc..
so users can start up drc directly
## Build from source
```bash
git clone git@mobike.io:database/drc.git
cd drc
make build
```
# Play with drc
There are two modes for drc
+ Manual mode
Operate pipeline manually
+ Cluster mode
Operate pipeline using rest-api 
## Dependencies
+ go
+ mysql
+ kafka
+ etcd
## Manual mode
There are two kinds of pipelines by now(may be more)
### Create a pipeline with producer only
+ Deploy Mysql and Kafka

Assume you have started required mysql and kafka instances
+ Update gravity configuration
```bash
config_file="gravity/config_dev.toml"
cp gravity/config_example/mysql-pb.toml $config_file
vim $config_file
```
+ Start gravity
```bash
./bin/gravity --config $config_file
```
+ verify
```bash
# use tidb-bench for generate mysql commands: https://github.com/pingcap/tidb-bench/tree/master/sysbench
# Prepare schema: source-db and source-table
./parallel-prepare.sh
# Fill in test data
./insert.sh
# Check gravity instance received test data successfully. gravity logs are expected to update
# synchronously as test data is written

# Check kafka received test data successfully. test data is expected to display on the stdout after 
# you executed the follow command
wget http://mirror.bit.edu.cn/apache/kafka/1.1.0/kafka_2.11-1.1.0.tgz
./kafka-simple-consumer-shell.sh --topic $topicName --broker-list $kafkaHost:$kafkaPort

```
### Create a pipeline with producer and consumer
+ Deploy Mysql and Kafka

Assume you have started required 2 mysql instances and 1 kafka instance
+ Update configurations
    + gravity
    ```bash
    gravity_config_file="gravity/config_dev.toml"
    cp gravity/config_example/mysql-pb.toml $gravity_config_file
    vim $gravity_config_file
    ```
    + nuclear
    ```bash
    nuclear_config_file="nuclear/config_dev.toml"
    cp nuclear/config_example/mysql-pb.toml $nuclear_config_file
    vim $nuclear_config_file
    ```
+ Start gravity
```bash
./bin/gravity --config $gravity_config_file
```
+ Start nuclear
```bash
./bin/nuclear --config $nuclear_config_file
```
+ verify
```bash
# use tidb-bench for generate mysql commands: https://github.com/pingcap/tidb-bench/tree/master/sysbench
# Prepare source schema: source-db and source-table from gravity conf
./parallel-prepare.sh
# Prepare target schema: target-schema and target-table-name from nuclear conf
./parallel-prepare.sh
# Fill in test data to source mysql instance
# ./insert.sh
# Check gravity instance received test data successfully. gravity logs are expected to update
# synchronously as test data is written
# Check kafka received test data successfully. test data is expected to display on the stdout after 
# you executed the follow command
wget http://mirror.bit.edu.cn/apache/kafka/1.1.0/kafka_2.11-1.1.0.tgz
./kafka-simple-consumer-shell.sh --topic $topicName --broker-list $kafkaHost:$kafkaPort
# After insert operation finished, check test data from source table and the one from target table
# are consistent: you can check data count, random rows. etc...
```
## Cluster mode
### Deploy a drc cluster
+ Deploy drc master
```bash
config_file=e2e/master_dev.toml
cp e2e/master.toml $config_file
vim $config_file
./bin/master --config $config_file
```
+ Registor a gravity worker to drc master
```bash
./bin/gravity --etcd-endpoints=http://127.0.0.1:2381 -http-addr=127.0.0.1:8081
```
+ Registor a nuclear worker to drc master
```bash
./bin/nuclear --etcd-endpoints=http://127.0.0.1:2381 -http-addr=127.0.0.1:8082
```
### Create a pipeline
#### Api
uri | type | params 
- | :-: | -: 
/pipeline | POST | see in Configuration
#### Configuration
`You can get the example in $drc_workspace/specs_example.json`
#### Example
```bash
# Create a pipeline. You will get a pipeline id if no error occurred
specs_config_file="specs_example_dev.json"
cp specs_example.json $specs_config_file
vim $specs_config_file
config=`cat $specs_config_file`
curl http://$drc_home_url/pipeline -X POST -H "Accept: application/json" -H "Content-type: application/json" -d "$config"
```
### Stop a pipeline
#### Api
uri | type | params 
- | :-: | -: 
/pipeline/$pipelineId/stop | POST | --
#### Example
```bash
# Stop a pipeline
curl http://$drc_home_url/pipeline/$pipelineId/stop -X POST
```
### Start a pipeline
#### Api
uri | type | params 
- | :-: | -: 
/pipeline/$pipelineId/start | POST | --
#### Example
```bash
# Start a pipeline
curl http://$drc_home_url/pipeline/$pipelineId/start -X POST
```
### Update a pipeline
#### Api
uri | type | params 
- | :-: | -: 
/pipeline | POST | see in Configuration
#### Configuration
see Configuration in Create a pipeline
#### Example
```bash
# Update a pipeline
specs_config_file="example_dev_update.json"
cp specs_example.json $specs_config_file
vim $specs_config_file
config=`cat $specs_config_file`
curl http://$drc_home_url/pipeline/$pipelineId -X POST -H "Accept: application/json" -H "Content-type: application/json" -d "$config"
```
### Query a pipeline
#### Api
#### Example
```bash
# Query a pipeline
curl http://$drc_home_url/pipeline/$pipelineId -X GET
```
# Contribute to drc
