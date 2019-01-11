# Configuration

This document introduces how to configure DRC Single Process and DRC Cluster respectively.

## Gravity Single Process

To configure Gravity Single Process, use the configuration file.

Gravity works in the plugin-based mode and each plugin has its own configuration. Currently, DRC supports the configuration file in the `toml` and `json` formats.

This section uses the configuration file in the `toml` format to describe configuration rules.

## Gravity Cluster

To configure Gravity Cluster, use Rest API to start the task. Make sure the Rest API and the `json` format of the configuration file are consistent.

For Gravity Cluster, the Web interface configuration is provided. For details about the options of Rest API, see the configuration file in the `toml` format.

## Note

For the following configurations, the `Input` and `Output` configurations are required:

- [Input Configuration](03-inputs-en.md)
- [Output Configuration](04-outputs-en.md)
- [Filter Configuration](05-filters-en.md)
- [Scheduler Configuration](06-scheduler-en.md)