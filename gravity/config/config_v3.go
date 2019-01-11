package config

import "encoding/json"

type PipelineConfigV3 struct {
	PipelineName    string          `mapstructure:"name" toml:"name" json:"name"`
	InputPlugin     InputConfig     `mapstructure:"input" toml:"input" json:"input"`
	FilterPlugins   []GenericConfig `mapstructure:"filters" toml:"filters" json:"filters,omitempty"`
	OutputPlugin    GenericConfig   `mapstructure:"output" toml:"output" json:"output"`
	SchedulerPlugin *GenericConfig  `mapstructure:"scheduler" toml:"scheduler" json:"scheduler,omitempty"`
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
	Type   string                 `mapstructure:"type"  json:"type"  toml:"type"`
	Mode   InputMode              `mapstructure:"mode" json:"mode" toml:"mode"`
	Config map[string]interface{} `mapstructure:"config"  json:"config"  toml:"config"`
}

type GenericConfig struct {
	Type   string                 `mapstructure:"type"  json:"type"  toml:"type"`
	Config map[string]interface{} `mapstructure:"config"  json:"config,omitempty"  toml:"config,omitempty"`
}
