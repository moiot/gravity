# Gravity
-------------------------
[![Build Status](https://travis-ci.org/moiot/gravity.svg?branch=master)](https://travis-ci.org/moiot/gravity.svg?branch=master)

![2.0 Product](docs/2.0/product.png)

Gravity is build for data replication. It is used to replicate dat between databases.

It is designed to be a customizable data replication tool that

- Supports multiple sources and destinations

- Supports Kubernetes-based PaaS platform to facilitate the maintenance tasks

## Application scenarios

- Data Bus: Use Change Data Capture (MySQL binlog, MongoDB Oplog) and batch scan to publish data to message queue like Kafka.
- Unidirectional data replication: Replicates data from one MySQL cluster to another MySQL cluster.
- Bidirectional data replication: Replicates data between two MySQL clusters bidirectionally.
- Synchronization of shards to the merged table: Synchronizes MySQL sharded tables to the merged table. You can specify the corresponding relationship between the source table and the target table.
- Online data mutation: Data can be changed during the replication. For example, rename the column, encrypt/decrypt data columns. 
## Features

### Data source support

Gravity supports the following data sources:

|   |   |
|---|---|
|  MySQL Binlog | ✅  | 
|  MySQL Scan |  ✅ |   
|  Mongo Oplog | ✅  | 
|  TiDB Binlog | 开发中  |
|  PostgreSQL WAL | 开发中  |

The support for the following items is in progress:

- TiDB binlogs
- PostgreSQL WAL logs

### Data output platform support

Gravity supports outputting data to the following platforms:

- Kafka 
- MySQL
- TiDB

The support for outputting data to MongoDB is in progress. 

### Data mutation support

Gravity supports the following data mutations:

- Data filtering
- Renaming columns
- Deleting columns

### Documentation

- [Architecture](docs/2.0/00-arch-en.md)
- [Quick Start](docs/2.0/01-quick-start-en.md)
- [Configuration](docs/2.0/02-config-index-en.md)
- [Deployment](https://github.com/moiot/gravity-operator)