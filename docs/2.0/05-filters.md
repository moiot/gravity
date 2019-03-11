Filter 定义了对 Input 消息的一些列变换操作。

Filter 是以数组的方式配置的，系统会按照顺序执行每一个 Filter。

当前支持如下几种 Filter：

- **reject** 忽略匹配的源端消息
- **delete-dml-column** 删除源端 DML 消息里的某些列
- **rename-dml-column** 重命名源端 DML 消息里的某些列


### reject

```toml
[[filters]]
type = "reject"

[filters.config]
match-schema = "test"
match-table = "test_table_*"
```

`reject` Filter 会拒绝所有匹配到的 Input 消息，这些消息不会发送到下一个 Filter，也不会发送给 Output。

上面的例子里，所有 `schema` 为 `test`，`table` 名字为 `test_table_*` 开头的消息都会被过滤掉。

```toml
[[filters]]
type = "reject"
[filters.config]
match-schema = "test"
match-dml-op = "delete"
```

上面的例子里，所有 `schema` 为 `test` 的 `delete` 类型 `DML` 都会被过滤掉。

### delete-dml-column
```toml
[[filters]]
type = "delete-dml-column"
[filters.config]
match-schema = "test"
match-table = "test_table"
columns = ["e", "f"]
```

`delete-dml-column` Filter 会删除匹配到的 Input 消息里的某些列。

上面的例子里，所有 `schema` 为 `test`, `table` 名字为 `test_table` 的 DML 消息，它们的 `e`, `f` 列都会被删除。

### rename-dml-column

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

`grpc-sidecar` Filter 会下载一个你指定的二进制文件并启动一个进程。你的这个程序需要实现一个 GRPC 的服务。一个 Golang 的例子在[这里](https://github.com/moiot/gravity-grpc-sidecar-filter-example)

目前 `grpc-sidecar` 只支持修改 `core.Msg.DmlMsg` 里的内容。

GRPC 协议的定义在[这里](https://github.com/moiot/gravity/blob/master/protocol/msgpb/message.proto)
```toml
[[filters]]
type = "grp-sidecar"
[filters.config]
match-schema = "test"
match-table = "test_table"
binary-url = "binary url that stores the binary"
name = "unique name of this plugin"
```