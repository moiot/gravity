# RFC： schema store


### 目的
Schema Storage 的目的是为了保存源数据库当前的 Schema，主要的目的是两个：
1. 解析当前的 binlog event 之后，需要知道 Column 的值和 Column 的名字的对应关系，才能包含完整语义的数据到 kafka
2. gravity 重启之后，需要知道当前所对应的 binlog 位置对应的 Schema

### 设计

基于以上两个目的，考虑把源数据库的 Schema 信息存放在数据库里。由于 Schema 的信息会随着 DDL 操作的发生而变化，所有需要有地方存储 Schema 的信息。

注意不能直接从源数据库拿 Schema，当处理 DDL 操作的时候，从源数据库拿到的 Schema 是当前时间的最新的 Schema，但不一定是 下一个要处理的 DML 语句对应的 Schema。

本设计中，利用 `schema-tracker` 跟踪源数据库的 Schema。`schema-tracker` 是一个独立的 MySQL 数据库，每次 DDL 变更操作到来的时候，都会在 `schema-tracker` 上执行一次。当前 Schema 可以通过 `show create table` 语句获取。




以下为数据库表结构的设计：

```
CREATE TABLE `mysql_schema_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `previous_schema_id` int(11)
  `binlog_position_id` text NOT NULL,
  `schema` text NOT NULL,
  `create_table_statement` text NOT NULL,
  `source_uuid` varchar(255) NOT NULL,
  `database_name` varchar(255) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `created_at`
  `updated_at`
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```

SchemaStore 接口：

- getSchema(binlogPositionId int) schema
  
  返回此  positionId 能找到的最新的 Schema。在 `mysql_schema_history` 中查找 `binlogPositionId` 对应的最新的  Schema
  
- processDDL(sql string, binlogPositionId int)

  存储此 `binlogPositionId` 对应的 Schema。此操作会先在 `schema-tracker` 里执行 DDL 语句，然后对相应的 table 查询 `show create table`，
  将返回的 语句存入 `create_table_statement`，并通过 `information_schema` 得到当前 schema, 存入 `mysql._schema_history.schema`。
  
  `create_table_statement` 语句可以方便我们调试。
  
 
`binlogPositionId` 连同 binlog 的位置保存在本地文件里持久化存储，每次更新 binlog 位置时会对 `binlogPositionId` 更新。`binlogPositionId`设计为自增的。


将来可以优化的地方：

- 能够处理 DDL 失败的情况。可以在 DDL 操作前 dump 一次 `schema_tracker` 的 Schema，操作完成后删除这个 dump，如果 DDL 失败，那么下次重启的时候发现有 dump 存在，可以重新再来

- binlogPosition 以及 binlog 的位置现在是存在文件里，实时更新。
  可以考虑存在数据库里，并且存储 binlog 位置的历史版本 (CheckPoint)，这样可以通过历史版本的 binlog 位置以及 Schema 回放某个 binlog 位置开始的操作。