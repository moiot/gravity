package routers

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/config"
	"testing"
)

type ElasticsearchServerAuth struct {
	Username string `mapstructure:"username" toml:"username" json:"username"`
	Password string `mapstructure:"password" toml:"password" json:"password"`
}

type ElasticsearchServerConfig struct {
	URLs    []string                 `mapstructure:"urls" toml:"urls" json:"urls"`
	Sniff   bool                     `mapstructure:"sniff" toml:"sniff" json:"sniff"`
	Auth    *ElasticsearchServerAuth `mapstructure:"auth" toml:"auth" json:"auth"`
	Timeout int                      `mapstructure:"timeout" toml:"timeout" json:"timeout"`
}

type EsModelPluginConfig struct {
	ServerConfig     *ElasticsearchServerConfig `mapstructure:"server" json:"server"`
	Routes           []map[string]interface{}   `mapstructure:"routes" json:"routes"`
	IgnoreBadRequest bool                       `mapstructure:"ignore-bad-request" json:"ignore-bad-request"`
}

func TestNewEsModelRoutes(t *testing.T) {

	cfg := config.NewConfig()

	if err := cfg.ConfigFromFile("../../../docs/2.0/example-mysql2esmodel.toml"); err != nil {
		println(err)
	}

	//if bs, err := json.Marshal(cfg.PipelineConfig.OutputPlugin); err != nil {
	//	println(err)
	//}else{
	//	fmt.Printf("%s\n", string(bs))
	//}

	pluginConfig := EsModelPluginConfig{}

	err := mapstructure.Decode(cfg.PipelineConfig.OutputPlugin.Config, &pluginConfig)
	if err != nil {
		fmt.Println(err)
	}

	if bs, err := json.Marshal(pluginConfig); err != nil {
		println(err)
	} else {
		fmt.Printf("%s\n", string(bs))
	}

	routers, err := NewEsModelRoutes(pluginConfig.Routes)

	fmt.Println(err)

	if bs, err := json.Marshal(routers); err != nil {
		println(err)
	} else {
		fmt.Printf("%s\n", string(bs))
	}

}
