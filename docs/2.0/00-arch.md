### 架构简介

**单进程架构**

单进程的 Gravity 采用基于插件的微内核架构，由各个插件围绕系统里的 `core.Msg` 结构实现输入到输出的整个流程。

各个插件有各自独立的配置选项。

![2.0 Arch Image](./single-process-160.png)

如上图所示，系统总共由这几个插件组成：

- **Input** 用来适配各种数据源，比如 MySQL 的 Binlog 并生成 `core.Msg`

- **Filter** 用来对 Input 所生成的数据流做数据变换操作，比如过滤某些数据，重命名某些列，对列加密

- **Output** 用来将数据写入目标，比如 Kafka, MySQL，**Output** 写入目标时，使用 **Router** 所定义的路由规则

- **Scheduler** 用来对 Input 生成的数据流调度，并使用 Output 写入目标；Scheduler 定义了当前系统支持的一致性特性（_当前默认的 Scheduler 支持同一行数据的修改有序_）

- **Matcher** 用来匹配 Input 生成的数据。**Filter** 和 **Router** 使用 **Matcher** 匹配数据

开发人员可以开发以上的几个插件类型，实现特定的需求。

`core.Msg` 的定义如下

```golang

type DDLMsg struct {
	Statement string
}

type DMLMsg struct {
	Operation DMLOp
	Data      map[string]interface{}
	Old       map[string]interface{}
	Pks       map[string]interface{}
	PkColumns []string
}

type Msg struct {
	Type      MsgType
	Host      string
	Database  string
	Table     string
	Timestamp time.Time

	DdlMsg *DDLMsg
	DmlMsg *DMLMsg
	...
}
```

**集群架构**

集群版本的 Gravity 原生支持 Kubernetes 上的集群部署，请查看[这里](https://github.com/moiot/gravity-operator)。

集群版本 DRC 提供 Rest API 创建创建数据同步任务，汇报状态。自带 Web 界面 (DRC Admin) 管理各个 DRC 任务。

