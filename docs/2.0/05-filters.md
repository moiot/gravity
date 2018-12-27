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
match-schema = "test"
match-table = "test_table_*"
```

`reject` Filter 会拒绝所有匹配到的 Input 消息，这些消息不会发送到下一个 Filter，也不会发送给 Output。

上面的例子里，所有 `schema` 为 `test`，`table` 名字为 `test_table_*` 开头的消息都会被过滤掉。

### delete-dml-column
```toml
[[filters]]
type = "delete-dml-column"
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
match-schema = "test"
match-table = "test_table"
from = ["a", "b"]
to = ["c", "d"]
```

