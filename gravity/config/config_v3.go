package config

import "encoding/json"

const PipelineConfigV3Version = "1"

type PipelineConfigV3 struct {
	PipelineName    string          `yaml:"name" toml:"name" json:"name"`
	Version         string          `yaml:"version" toml:"version" json:"version"`
	InputPlugin     InputConfig     `yaml:"input" toml:"input" json:"input"`
	FilterPlugins   []GenericConfig `yaml:"filters" toml:"filters" json:"filters,omitempty"`
	OutputPlugin    GenericConfig   `yaml:"output" toml:"output" json:"output"`
	SchedulerPlugin *GenericConfig  `yaml:"scheduler" toml:"scheduler" json:"scheduler,omitempty"`
}

func (c *PipelineConfigV3) SetDefault() {
	if c.Version == "" {
		c.Version = PipelineConfigV3Version
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
	Batch       InputMode = "batch"
	Stream      InputMode = "stream"
	Replication InputMode = "replication" // scan + binlog
)

type InputMode string

type InputConfig struct {
	Type   string                 `yaml:"type"  json:"type"  toml:"type"`
	Mode   InputMode              `yaml:"mode" json:"mode" toml:"mode"`
	Config map[string]interface{} `yaml:"config"  json:"config"  toml:"config"`
}

type GenericConfig struct {
	Type   string                 `yaml:"type"  json:"type"  toml:"type"`
	Config map[string]interface{} `yaml:"config"  json:"config,omitempty"  toml:"config,omitempty"`
}
