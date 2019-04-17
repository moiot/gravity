package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

type PipelineConfigV2 struct {
	PipelineName     string                 `mapstructure:"name" toml:"name" json:"name"`
	InputPlugins     map[string]interface{} `toml:"input" json:"input" mapstructure:"input"`
	FilterPlugins    []interface{}          `mapstructure:"filters" toml:"filters" json:"filters,omitempty"`
	OutputPlugins    map[string]interface{} `mapstructure:"output" toml:"output" json:"output"`
	SchedulerPlugins map[string]interface{} `mapstructure:"scheduler" toml:"scheduler" json:"scheduler,omitempty"`
}

func (c *PipelineConfigV2) IsV3() bool {
	_, ok := c.InputPlugins["type"]
	if !ok {
		log.Warn("received v2 config")
	}
	return ok
}

func (c *PipelineConfigV2) ToV3() PipelineConfigV3 {
	ret := PipelineConfigV3{
		PipelineName: c.PipelineName,
	}

	for k, v := range c.InputPlugins {
		ret.InputPlugin.Type = k
		ret.InputPlugin.Config = v.(map[string]interface{})
		if k == "mysql" {
			ret.InputPlugin.Mode = InputMode(ret.InputPlugin.Config["mode"].(string))
		} else {
			ret.InputPlugin.Mode = Stream
		}
	}

	for _, f := range c.FilterPlugins {
		m := f.(map[string]interface{})
		ff := GenericPluginConfig{
			Type: m["type"].(string),
		}
		delete(m, "type")
		ff.Config = m
		ret.FilterPlugins = append(ret.FilterPlugins, ff)
	}

	for k, v := range c.OutputPlugins {
		ret.OutputPlugin.Type = k
		ret.OutputPlugin.Config = v.(map[string]interface{})
	}

	for k, v := range c.SchedulerPlugins {
		ret.SchedulerPlugin = &GenericPluginConfig{
			Type:   k,
			Config: v.(map[string]interface{}),
		}
	}

	return ret
}

func DecodeTomlString(s string) (*PipelineConfigV2, error) {
	cfg := &PipelineConfigV2{}
	if _, err := toml.Decode(s, cfg); err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}
