### 配置文件

单进程的 DRC 使用配置文件来配置。

DRC 是基于插件的微内核模式，各个插件有自己独立的配置。目前 DRC 支持以 `toml` 格式和 `json` 格式作为配置文件来配置。

本节的描述中，为了方便起见，统一使用 `toml` 格式的配置文件描述配置规则。


### Rest API

集群方式部署的 DRC 集群使用 Rest API 来启动任务，Rest API 和配置文件的 `json` 格式保持一致。


集群方式部署提供 Web 界面配置，因此本节不再描述 Rest API 的各个选项，请参考 `toml` 格式的配置文件描述即可。


-------------------


配置文件最少需要提供 `Input` 和 `Output` 的配置。

- [Input 配置](03-inputs.md)
- [Output 配置](04-outputs.md)
- [Filter 配置](05-filters.md)
- [Scheduler 配置](06-scheduler.md)