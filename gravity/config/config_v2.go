package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type PipelineConfigV2 struct {
	PipelineName     string                 `mapstructure:"name" toml:"name" json:"name"`
	InputPlugins     map[string]interface{} `toml:"input" json:"input" mapstructure:"input"`
	FilterPlugins    []interface{}          `mapstructure:"filters" toml:"filters" json:"filters,omitempty"`
	OutputPlugins    map[string]interface{} `mapstructure:"output" toml:"output" json:"output"`
	SchedulerPlugins map[string]interface{} `mapstructure:"scheduler" toml:"scheduler" json:"scheduler,omitempty"`
}

func DecodeTomlString(s string) (*PipelineConfigV2, error) {
	cfg := &PipelineConfigV2{}
	if _, err := toml.Decode(s, cfg); err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}
