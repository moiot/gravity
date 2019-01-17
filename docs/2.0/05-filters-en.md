---
title: Filter Configuration
summary: Learn how to configure Filters.
---

# Filter Configuration

Filter defines the column mutation operations on the Input message. It is configured in the array and the system executes each Filter in order.

Currently, DRC supports the following Filter plugins:

- `reject`: Ignores specific source messages.
- `delete-dml-column`: Deletes specific columns in the source DML messages.
- `rename-dml-column`: Renames specific columns in the source DML messages.

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

For the above configuration, the "a" column is renamed to "c" and the "b" column is renamed to "d".