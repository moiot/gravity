package config

import (
	"flag"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/logutil"
	"github.com/moiot/gravity/pkg/utils"
)

type Config struct {
	*flag.FlagSet `json:"-"`
	Log           logutil.LogConfig `toml:"log" json:"log"`
	ConfigFile    string            `toml:"-" json:"-"`
	PadderConfig  PadderConfig      `toml:"padder" json:"padder"`
	Version       bool
	PreviewMode   bool
}

type PadderConfig struct {
	BinLogList   []string     `toml:"binlog-list" json:"binlog-list"`
	MySQLConfig  *MySQLConfig `toml:"mysql" json:"mysql"`
	EnableDelete bool         `toml:"enable-delete" json:"enable-delete"`
}

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("padder", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.BoolVar(&cfg.PreviewMode, "preview", false, "preview mode")
	fs.StringVar(&cfg.Log.Level, "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.Log.Format, "log-format", "json", "log format")
	return cfg
}

func (cfg *Config) ParseCmd(arguments []string) error {
	return cfg.FlagSet.Parse(arguments)
}

type MySQLConfig struct {
	Target        *utils.DBConfig            `toml:"target" json:"target"`
	StartPosition *utils.MySQLBinlogPosition `toml:"start-position" json:"start-position"`
}

func (c *Config) CreateConfigFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func CreateConfigFromString(configString string) (*Config, error) {
	cfg := &Config{}
	_, err := toml.Decode(configString, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cfg, nil
}

func Validate(cfg PadderConfig) error {
	if len(cfg.BinLogList) == 0 {
		return errors.NotValidf("bin log list are required. config is")
	}
	if cfg.MySQLConfig == nil {
		return errors.NotValidf("mysql config is required. config is")
	}
	// either LogPos&&LogName or GTID?
	if cfg.MySQLConfig.StartPosition == nil {
		return errors.NotValidf("start position is required. config is")
	}
	if cfg.MySQLConfig.Target == nil {
		return errors.NotValidf("mysql target is required. config is")
	}
	if err := cfg.MySQLConfig.Target.ValidateAndSetDefault(); err != nil {
		return errors.NotValidf("mysql config validates failed. %v, config is", err)
	}
	if cfg.MySQLConfig.Target.Schema == "" {
		return errors.NotValidf("mysql schema is required. config is")
	}
	return nil
}
