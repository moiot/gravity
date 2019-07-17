---
title: Gravity Quick Start Guide
summary: Learn how to quickly start Gravity.
---

# Gravity Quick Start Guide

This document takes synchronizing the data of the local MySQL instance and data subscription as examples to introduce how to quickly start Gravity.

## Step 1: Configure the MySQL environment

1. Refer to [Configure the MySQL environment](https://github.com/moiot/gravity/blob/master/docs/2.0/03-inputs-en.md#mysql-environment-configuration) to configure the MySQL environment.

2. Create the table that needs to be synchronized in the source MySQL database and the target MySQL database:

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

## Step 2: Compile (optional, docker image is ready for use)

[Configure the Go environment](https://golang.org/doc/install) and compile:

```bash
mkdir -p $GOPATH/src/github.com/moiot/ && cd $GOPATH/src/github.com/moiot/

git clone https://github.com/moiot/gravity.git

cd gravity && make

```

## Step 3: Write config file 

You can choose to synchronize data between two MySQL clusters or from MySQL to Kafka.

- Synchronize data between two MySQL clusters

    Create the configuration file, as shown in the following file `config.toml`:

        ```toml
        # name (required)
        name = "mysql2mysqlDemo"
        version = "1.0"
        
        # optional. 
        # database name used to store position, heartbeat, etc. 
        # default to "_gravity"
        internal-db-name = "_gravity"
        
        #
        # The definition of Input. `mysqlbinlog` is used for this definition.
        #
        [input]
        type = "mysql"
        mode = "stream"
        [input.config.source]
        host = "127.0.0.1"
        username = "root"
        password = ""
        port = 3306
        location = "Local"

        #
        # The definition of Output. `mysql` is used for this definition.
        #
        [output]
        type = "mysql"
        [output.config.target]
        host = "127.0.0.1"
        username = "root"
        password = ""
        port = 3306
        location = "Local"

        # The definition of the routing rule
        [[output.config.routes]]
        match-schema = "test"
        match-table = "test_source_table"
        target-schema = "test"
        target-table = "test_target_table"
        ```

- Synchronize data from MySQL to Kafka

    Create the configuration file, as shown in the following file `config.toml`:

        ```toml
        
        name = "mysql2kafkaDemo"
 
        #
        # The definition of Input. `mysqlbinlog` is used for this definition.
        #
        [input]
        type = "mysql"
        mode = "stream"
        [input.config.source]
        host = "127.0.0.1"
        username = "root"
        password = ""
        port = 3306
        location = "Local"

        #
        # The definition of Output. `mysql` is used for this definition.
        #
        [output]
        type = "async-kafka"
        [output.config.kafka-global-config]
        broker-addrs = ["127.0.0.1:9092"]
        mode = "async"

        # The routing definition of Kafka
        [[output.config.routes]]
        match-schema = "test"
        match-table = "test_source_table"
        dml-topic = "test"
        ```

## Step 4: Start Gravity
From binary
```bash
bin/gravity -config mysql2mysql.toml
```
or docker
```bash
docker run -v ${PWD}/config.toml:/etc/gravity/config.toml -d --net=host moiot/gravity:latest
```