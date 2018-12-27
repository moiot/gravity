---
title: DRC Quick Start Guide
summary: Learn how to quickly start DRC.
---

# DRC Quick Start Guide

This document takes synchronizing the data of the local MySQL instance and data subscription as examples to introduce how to quickly start DRC.

## Step 1: Configure the MySQL environment

1. Refer to [Configure the MySQL environment](configure-the-mysql-environment.md) to configure the MySQL environment.

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

## Step 2: Compile (TODO: Download binary from GitHub after it is open sourced)

[Configure the Go environment](https://golang.org/doc/install) and compile:

```bash
mkdir -p $GOPATH/src/github.com/moiot/ && cd $GOPATH/src/github.com/moiot/

git clone https://github.com/moiot/gravity.git

cd gravity && make

```

## Step 3: Synchronize data 

You can choose to synchronize data between two MySQL clusters or from MySQL to Kafka.

- Synchronize data between two MySQL clusters

    1. Create the configuration file, as shown in the following file `mysql2mysql.toml`:

        ```toml
        # name (required)
        name = "mysql2mysqlDemo"

        #
        # The definition of Input. `mysqlbinlog` is used for this definition.
        #
        [input.mysqlbinlog.source]
        host = "127.0.0.1"
        username = "root"
        password = ""
        port = 3306
        location = "Local"

        #
        # The definition of Output. `mysql` is used for this definition.
        #
        [output.mysql.target]
        host = "127.0.0.1"
        username = "root"
        password = ""
        port = 3306
        location = "Local"

        # The definition of the routing rule
        [[output.mysql.routes]]
        match-schema = "test"
        match-table = "test_source_table"
        target-schema = "test"
        target-table = "test_target_table"

        #
        # The definition of Scheduler. `scheduler` is used for this definition by default.
        #
        [scheduler.batch-table-scheduler]
        nr-worker = 2
        batch-size = 1
        queue-size = 1024
        sliding-window-size = 1024
        ```

    2. Enable `drc`.

        ```bash
        bin/drc -config mysql2mysql.toml
        ```

    3. After the incremental synchronization between `test_source_table` and `test_target_table` begins, you can insert data in the source cluster and then you will see the change in the target cluster.

- Synchronize data from MySQL to Kafka

    1. Create the configuration file, as shown in the following file `mysql2kafka.toml`:

        ```toml
        name = "mysql2kafkaDemo"

        #
        # The definition of Input. `mysqlbinlog` is used for this definition.
        #
        [input.mysqlbinlog.source]
        host = "127.0.0.1"
        username = "root"
        password = ""
        port = 3306
        location = "Local"

        #
        # The definition of Output. `mysql` is used for this definition.
        #
        [output.async-kafka.kafka-global-config]
        broker-addrs = ["127.0.0.1:9092"]
        mode = "async"

        # The routing definition of Kafka
        [[output.async-kafka.routes]]
        match-schema = "test"
        match-table = "test_source_table"
        dml-topic = "test"

        #
        # The definition of Scheduler. `scheduler` is used for this definition by default.
        #
        [scheduler.batch-table-scheduler]
        nr-worker = 1
        batch-size = 1
        queue-size = 1024
        sliding-window-size = 1024
        ```

    2. Enable `drc`:

        ```bash
        bin/drc -config mysql2kafka.toml
        ```

    3. The data mutation of `test`.`test_source_table` table in the MySQL cluster is sent to `test` of Kafka.