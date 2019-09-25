---
title: Filter Configuration
summary: Learn how to configure Filters.
---

# Filter Configuration

Filter defines the column mutation operations on the Input message. It is configured in the array and the system executes each Filter in order.

Currently, Gravity supports the following Filter plugins:

- `reject`: A blacklist filter that rejects specific messages.
- `accept`: A whitelist filter that only accept specific messages.
- `delete-dml-column`: Deletes specific columns in the source DML messages.
- `rename-dml-column`: Renames specific columns in the source DML messages.
- `grpc-sidecar`: Filters that deployed as a grpc server that can do anything you like.

## `reject` configuration

The `reject` Filter rejects specific Input messages. Then these messages will not be the next Filter or Output.

```toml
[[filters]]
type = "reject"
[filters.config]
match-schema = "test"
match-table = "test_table_*"
```

For the above configuration, all the messages with `schema` set to `test` and `table` set to `test_table_*` are filtered.

```toml
[[filters]]
type = "reject"
[filters.config]
match-schema = "test"
match-dml-op = "delete"
```

For the above configuration, all the `delete` `DML` message with `schema` set to `test` are rejected.

## `accept` configuration

The accept is a whitelist filter that accepts matched input messages.

```toml
[[filters]]
type = "accept"
[filters.config]
match-schema = "test"
match-table = "test_table_*"
```

For the above configuration, only handle messages with `schema` set to `test` and `table` set to `test_table_*`.

## `delete-dml-column` configuration

The `delete-dml-column` Filter deletes specific columns in the Input message.

```toml
[[filters]]
type = "delete-dml-column"
[filters.config]
match-schema = "test"
match-table = "test_table"
columns = ["e", "f"]
```

For the above configuration, "e" and "f" columns in all the DML messages with `schema` set to `test` and `table` set to `test_table` are deleted.

## `rename-dml-column` configuration

The `rename-dml-column` Filter renames specific columns in the source DML messages.

```toml
[[filters]]
type = "rename-dml-column"
[filters.config]
match-schema = "test"
match-table = "test_table"
from = ["a", "b"]
to = ["c", "d"]
```

## `grpc-sidecar`

The `grpc-sidecar` Filter will download the binary you specified and run the binary. This binary
should start a GRPC server that talks with filter plugin's protocol. An example of the filter
implemented in Golang can be found [here](https://github.com/moiot/gravity-grpc-sidecar-filter-example)

`grpc-sidecar` only supports the manipulation of `core.Msg.DmlMsg` right now.

The protocol of the GRPC plugin can be found [here](https://github.com/moiot/gravity/blob/master/protocol/msgpb/message.proto)
```toml
[[filters]]
type = "grpc-sidecar"
[filters.config]
match-schema = "test"
match-table = "test_table"
binary-url = "binary url that stores the binary"
name = "unique name of this plugin"
```