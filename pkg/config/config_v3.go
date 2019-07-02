package config

import (
	"encoding/json"

	"github.com/juju/errors"
)

const PipelineConfigV3Version = "1.0"
const defaultInternalDBName = "_gravity"

type PipelineConfigV3 struct {
	PipelineName    string                `yaml:"name" toml:"name" json:"name"`
	InternalDBName  string                `yaml:"internal-db-name" toml:"internal-db-name" json:"internal-db-name"`
	Version         string                `yaml:"version" toml:"version" json:"version"`
	InputPlugin     InputConfig           `yaml:"input" toml:"input" json:"input"`
	FilterPlugins   []GenericPluginConfig `yaml:"filters" toml:"filters" json:"filters,omitempty"`
	OutputPlugin    GenericPluginConfig   `yaml:"output" toml:"output" json:"output"`
	SchedulerPlugin *GenericPluginConfig  `yaml:"scheduler" toml:"scheduler" json:"scheduler,omitempty"`
}

func (c *PipelineConfigV3) SetDefault() {
	if c.Version == "" {
		c.Version = PipelineConfigV3Version
	}

	if c.InternalDBName == "" {
		c.InternalDBName = defaultInternalDBName
	}
}

func (c *PipelineConfigV3) DeepCopy() PipelineConfigV3 {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	ret := PipelineConfigV3{}
	if err = json.Unmarshal(b, &ret); err != nil {
		panic(err)
	}
	return ret
}

const (
	Unknown     InputMode = "unknown"
	Batch       InputMode = "batch"
	Stream      InputMode = "stream"
	Replication InputMode = "replication" // scan + binlog
)

type InputMode string

func (mode InputMode) Valid() error {
	if mode == Batch || mode == Stream || mode == Replication {
		return nil
	} else {
		return errors.Errorf("invalid mode: %v", mode)
	}
}

type InputConfig struct {
	Type   string                 `yaml:"type"  json:"type"  toml:"type"`
	Mode   InputMode              `yaml:"mode" json:"mode" toml:"mode"`
	Config map[string]interface{} `yaml:"config"  json:"config"  toml:"config"`
}

type GenericPluginConfig struct {
	Type   string                 `yaml:"type"  json:"type"  toml:"type"`
	Config map[string]interface{} `yaml:"config"  json:"config,omitempty"  toml:"config,omitempty"`
}
