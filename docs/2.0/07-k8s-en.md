---
title: DRC Cluster Operations Guide
summary: This document introduces how to deploy, configure and upgrade a DRC cluster.
---

# DRC Cluster Operations Guide

This document introduces how to deploy, configure and upgrade a DRC cluster. 

## DRC Cluster Deployment

### Prerequisites

- Kubernetes 1.11+ with CRD status subroutine
- [helm](https://helm.sh/)

### Install DRC Cluster

```bash
$ cd deploy/k8s/drc-operator
$ helm install --name drc-operator ./
```

This chart bootstraps a drc-operator deployment on a Kubernetes cluster using the Helm package manager.

## DRC Cluster Configuration

The following table shows the configurable parameters of the drc-operator chart and their default values. See `deploy/k8s/drc-operator/values.yaml`.

Parameter | Description | Default Value
--- | --- | ---
`tags.servicemonitor` | Installs service monitor for default prometheus-operator | `false`
`deploymentRules`| Array of deployment rules which control DRC deployment versions | See [`DeploymentRules`](#DeploymentRules)  
`operator.image.repository`| Image of the operator | `docker.mobike.io/database/drc/operator`
`operator.image.tag`| Image tag of the operator | `156a28a4`
`admin.image.repository`| Image of admin | `docker.mobike.io/database/drc-admin`
`admin.image.tag`| Image tag of admin | `59d44f7c`
`admin.service.nodePort`| Node port of the admin service | `30066`


By defining `DeploymentRule`, a DRC cluster is divided into multiple groups based on the pipeline name and these groups can use different DRC versions.

The default rule is as follows:

```yaml
  - group: "default"
    pipelines: ["*"]
    image: "docker.mobike.io/database/drc/drc:156a28a4"
    command: ["/drc", "-config=/etc/drc/config.json"]
```

Parameter | Type | Description
--- | --- | ---
`group`| String | Name of the rule
`pipelines`| String array | Global expression of the matched pipeline name
`image` | String | Image (including tag) of matched pipeline deployment. It will be written to pod template's container image field
`command` | String array | Command of running matched pipeline. It will be written to pod template's container command field

## DRC Cluster Upgrade

To upgrade a DRC cluster, perform the following steps:

1. Modify the `values.yaml` file in the chart.
2. Use [helm](https://helm.sh/) to upgrade the cluster.