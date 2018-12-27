当前支持的 Scheduler Plugin 只有一种

- **batch-table-scheduler** 保证一个表内，由主键内容定义的同一行数据的更新操作有序

### batch-table-scheduler

```toml
#
# batch-table-scheduler 配置
# - 可选
#
[scheduler.batch-table-scheduler]
# 默认值 1
nr-worker = 1

# 默认值 1
batch-size = 1

# 默认值 1024
queue-size = 1024

# 默认值 10240
sliding-window-size = 10240
```

batch-table-scheduler 使用 worker pool 的方式调用 Output 定义的接口。

`nr-worker` 是 worker 的数目；

batch-table-scheduler 按照 batch 来使用 Output，它保证一个 batch 有相同的 core.Msg.Table

`batch-size` 是 batch 大小；


batch-table-scheduler 使用 sliding window 保证 Input 的位点按顺序保存。

`sliding-window-size` 是 sliding window 大小。