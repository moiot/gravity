# DRC

DRC (Data Replication Center) is an open source data replication component of Mobike. It is used to synchronize the full data and incremental data and send the data mutation to the message queue.

It is designed to be a customizable data replication tool that

- Supports multiple data sources, data output platforms, and data consistency requirements
- Supports Kubernetes-based PaaS platform to facilitate the maintenance tasks

## Architecture

[DRC 2.0 Architecture](docs/2.0/drc-architecture.png)

## Application scenarios

- Big data bus: Sends the data mutation of the MySQL binlog and MongoDB Oplog to Kafka for consumption in the downstream.
- Unidirectional data synchronization: Synchronizes the full/incremental data from one MySQL cluster to another MySQL cluster.
- Bidirectional data synchronization: Synchronizes the incremental data between two MySQL clusters bidirectionally. Chained replication can be avoided in the synchronization process.
- Synchronization of shards to the merged table: Synchronizes MySQL sharded tables to the merged table. You can specify the corresponding relationship between the source table and the target table.
- Online data mutation: Supports heterogeneous schema mutation in the synchronization process.

## Features

### Data source support

DRC supports the following data sources:

- MySQL binlogs
- MySQL full data
- MongoDB Oplogs

The support for the following items is in progress:

- TiDB binlogs
- PostgreSQL WAL logs

### Data output platform support

DRC supports outputting data to the following platforms:

- Kafka 
- MySQL
- TiDB

The support for outputting data to MongoDB is in progress. 

### Data mutation support

DRC supports the following data mutations:

- Data filtering
- Renaming columns
- Deleting columns

### Documentation

- [Architecture](docs/2.0/00-arch-en.md)
- [Quick Start](docs/2.0/01-quick-start-en.md)
- [Configuration](docs/2.0/02-config-index-en.md)
- [Deployment](docs/2.0/07-k8s-en.md)