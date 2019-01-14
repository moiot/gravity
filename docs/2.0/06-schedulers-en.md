---
title: Scheduler Configuration
summary: Learn how to configure Scheduler.
---

# Scheduler Configuration

Currently, DRC supports only one Scheduler plugin:

- `batch-table-scheduler`: Guarantees that update operations on the data in the same row defined by the primary key are performed in order.

## `batch-table-scheduler` configuration

```toml
#
# `batch-table-scheduler` configuration
# Optional
#
[scheduler]
type = "batch-table-scheduler"

[scheduler.config]
# "1" by default
nr-worker = 1

# "1" by default
batch-size = 1

# "1024" by default
queue-size = 1024

# "10240" by default
sliding-window-size = 10240
```

In the above configuration:

- `batch-table-scheduler` uses the worker pool method to call the interface defined by Output.

    - `nr-worker` is the number of workers.

- `batch-table-scheduler` uses Output in the unit of batch and guarantees that one batch has the same `core.Msg.Table`.

    - `batch-size` is the size of a batch.

- `batch-table-scheduler` uses the sliding window to guarantee the positions in Input are saved in order.

    - `sliding-window-size` is the size of a sliding window.