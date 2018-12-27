---
title: DRC Configuration
summary: Learn how to configure DRC Single Process and DRC Cluster.
---

# DRC Configuration

This document introduces how to configure DRC Single Process and DRC Cluster respectively.

## Configuration of DRC Single Process

To configure DRC Single Process, use the configuration file.

DRC works in the plugin-based microkernel mode and each plugin has its own configuration. Currently, DRC supports the configuration file in the `toml` and `json` formats.

This section uses the configuration file in the `toml` format to describe configuration rules.

## Configuration of DRC Cluster

To configure DRC Cluster, use Rest API to start the task. Make sure the Rest API and the `json` format of the configuration file are consistent.

For DRC Cluster, the Web interface configuration is provided. For details about the options of Rest API, see the configuration file in the `toml` format.

## Note

For the following configurations, the `Input` and `Output` configurations are required:

- [Input Configuration](03-inputs-en.md)
- [Output Configuration](04-outputs-en.md)
- [Filter Configuration](05-filters-en.md)
- [Scheduler Configuration](06-scheduler-en.md)