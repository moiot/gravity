package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/logutil"
)

var DefaultBinlogSyncerTimeout = "10s"

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	EtcdEndpoints string `toml:"etcd-endpoints" json:"etcd-endpoints"`

	PipelineConfig PipelineConfigV3 `toml:"pipeline" json:"pipeline"`

	// Log related configuration.
	Log logutil.LogConfig `toml:"log" json:"log"`

	HttpAddr string `toml:"http-addr" json:"http-addr"`

	PositionFile         string `toml:"position-file" json:"position-file"`
	BlockProfileRate     int    ` toml:"-" json:"-"`
	MutexProfileFraction int    ` toml:"-" json:"-"`
	ConfigFile           string `toml:"-" json:"-"`
	ClearPosition        bool   `toml:"-" json:"-"`
	Version              bool
}

type PipelineConfig struct {
	PipelineName string `toml:"name" json:"name"`

	// Deprecated!
	// DetectTxn txn is used in: bi-directional transfer, dynamic route
	DetectTxn bool `toml:"detect-txn" json:"detect-txn"`

	// UniqueSourceName name of the server
	UniqueSourceName string `toml:"unique-source-name" json:"unique-source-name"`

	Input        string `toml:"input" json:"input"`
	Output       string `toml:"output" json:"output"`
	OutputFormat string `toml:"output-format" json:"output-format"`

	MongoConfig      *MongoConfigs     `toml:"mongo" json:"mongo"`
	MySQLConfig      *MySQLConfig      `toml:"mysql" json:"mysql"`
	SourceTiDBConfig *SourceTiDBConfig `toml:"source-tidb" json:"source-tidb"`
	SourceProbeCfg   *SourceProbeCfg   `toml:"source-probe-config" json:"source-probe-config"`

	KafkaGlobalConfig *KafkaGlobalConfig `toml:"kafka-global" json:"kafka-global"`

	//
	// RouteMode, DynamicKafkaRouteConfig, StaticKafkaRouteConfig, and DBRoutes
	// are route related configuration
	RouteMode string `toml:"route-mode" json:"route-mode"`
	// DynamicKafkaRouteConfig *router.DynamicKafkaRouteConfig `toml:"dynamic-route-config" json:"dynamic-route-config"`
	// StaticKafkaRouteConfig  *router.StaticKafkaRouteConfig  `toml:"static-route-config" json:"static-route-config"`
	// DBRoutes                []router.DBRouteConfig          `toml:"db-routes" json:"db-routes"`

	TableConfig []*TableConfig `toml:"table-config" json:"table-config"`

	TargetMySQL *DBConfig `toml:"target-mysql" json:"target-mysql"`

	TargetMySQLWorkerCfg *TargetMySQLWorkerConfig `toml:"target-mysql-worker" json:"target-mysql-worker"`

	// WorkerPoolConfig *worker_pool.WorkerPoolConfig `toml:"worker-pool-config" json:"worker-pool-config"`

	//
	// internal configurations that is not exposed to users
	//
	DisableBinlogChecker bool   `toml:"-" json:"-"`
	DebugBinlog          bool   `toml:"-" json:"-"`
	BinlogSyncerTimeout  string `toml:"-" json:"-"`
}

type SourceKafkaConfig struct {
	BrokerConfig KafkaGlobalConfig    `mapstructure:"brokers" toml:"brokers" json:"brokers"`
	GroupID      string               `mapstructure:"group-id" toml:"group-id" json:"group-id"`
	Topics       []string             `mapstructure:"topics" toml:"topics" json:"topics"`
	ConsumeFrom  string               `mapstructure:"consume-from" toml:"consume-from" json:"consume-from"`
	Common       KafkaCommonConfig    `mapstructure:"common" toml:"common" json:"common"`
	Consumer     *KafkaConsumerConfig `mapstructure:"consumer" toml:"consumer" json:"consumer"`
}

type SourceProbeCfg struct {
	SourceMySQL *DBConfig `mapstructure:"mysql" toml:"mysql" json:"mysql"`
	Annotation  string    `mapstructure:"annotation" toml:"annotation" json:"annotation"`
}

type MongoPosition bson.MongoTimestamp

type MongoSource struct {
	MongoConnConfig *MongoConnConfig `mapstructure:"source" toml:"source" json:"source"`
	StartPosition   *MongoPosition   `mapstructure:"start-position" toml:"start-position" json:"start-position"`
}
type MongoConfigs struct {
	MongoSources   []MongoSource    `toml:"mongo-sources" json:"mongo-sources"`
	PositionSource *MongoConnConfig `toml:"position-conn" json:"position-conn"`
	GtmConfig      *GtmConfig       `toml:"gtm-config" json:"gtm-config"`
}

type MySQLConfig struct {
	IgnoreBiDirectionalData bool                 `mapstructure:"ignore-bidirectional-data" toml:"ignore-bidirectional-data" json:"ignore-bidirectional-data"`
	Source                  *DBConfig            `mapstructure:"source" toml:"source" json:"source"`
	SourceSlave             *DBConfig            `mapstructure:"source-slave" toml:"source-slave" json:"source-slave"`
	StartPosition           *MySQLBinlogPosition `mapstructure:"start-position" toml:"start-position" json:"start-position"`
}

type SourceTiDBConfig struct {
	SourceDB    *DBConfig          `mapstructure:"source-db" toml:"source-db" json:"source-db"`
	SourceKafka *SourceKafkaConfig `mapstructure:"source-kafka" toml:"source-kafka" json:"source-kafka"`
	// OffsetStoreConfig       *SourceProbeCfg    `mapstructure:"offset-store" toml:"offset-store" json:"offset-store"`
	PositionRepo            *GenericPluginConfig `mapstructure:"position-repo" toml:"position-repo" json:"position-repo"`
	IgnoreBiDirectionalData bool                 `mapstructure:"ignore-bidirectional-data" toml:"ignore-bidirectional-data" json:"ignore-bidirectional-data"`
	FailOnTxnTags           []string             `mapstructure:"fail-on-txn-tags" toml:"fail-on-txn-tags" json:"fail-on-txn-tags"`
}

type GtmConfig struct {
	UseBufferDuration bool `mapstructure:"use-buffer-duration" toml:"use-buffer-duration" json:"use-buffer-duration"`
	BufferSize        int  `mapstructure:"buffer-size" toml:"buffer-size" json:"buffer-size"`
	ChannelSize       int  `mapstructure:"channel-size" toml:"channel-size" json:"channel-size"`
	BufferDurationMs  int  `mapstructure:"buffer-duration-ms" toml:"buffer-duration-ms" json:"buffer-duration-ms"`
}

type MongoConnConfig struct {
	Host     string `mapstructure:"host" toml:"host" json:"host"`
	Port     int    `mapstructure:"port" toml:"port" json:"port"`
	Username string `mapstructure:"username" toml:"username" json:"username"`
	Password string `mapstructure:"password" toml:"password" json:"password"`
	Database string `mapstructure:"database" toml:"database" json:"database"`
	Direct   bool   `mapstructure:"direct" toml:"direct" json:"direct"`
}

func (cfg MongoConnConfig) URI() string {
	username := ""
	if cfg.Username != "" {
		username = cfg.Username
	}

	password := ""
	if cfg.Password != "" {
		password = cfg.Password
	}

	host := "localhost"
	if cfg.Host != "" {
		host = cfg.Host
	}

	port := 27017
	if cfg.Port != 0 {
		port = cfg.Port
	}

	db := cfg.Database

	var url string
	if username == "" || password == "" {
		url = fmt.Sprintf("mongodb://%s:%d/%s", host, port, db)
	} else {
		url = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s", username, password, host, port, db)
	}

	//If not specified, the connection will timeout, probably because the replica set has not been initialized yet.
	if cfg.Direct {
		url += "?connect=direct"
	}

	return url
}

type TargetMySQLWorkerConfig struct {
	EnableDDL          bool     `toml:"enable-ddl" json:"enable-ddl"`
	UseBidirection     bool     `toml:"use-bidirection" json:"use-bidirection"`
	UseShadingProxy    bool     `toml:"use-shading-proxy" json:"use-shading-proxy"`
	SQLExecutionEngine string   `toml:"sql-execution-engine" json:"sql-execution-engine"`
	Plugins            []string `toml:"plugins" json:"plugins"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.PipelineConfig = PipelineConfigV3{}
	cfg.FlagSet = flag.NewFlagSet("gravity", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.Version, "V", false, "print version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.Log.Level, "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.Log.Format, "log-format", "json", "log format")
	fs.StringVar(&cfg.HttpAddr, "http-addr", ":8080", "http-addr")
	fs.IntVar(&cfg.BlockProfileRate, "block-profile-rate", 0, "set block profile rate")
	fs.IntVar(&cfg.MutexProfileFraction, "mutex-profile-fraction", 0, "set mutex profile fraction")
	return cfg
}

func LoadConfigFromFile(path string) *Config {
	cfg := &Config{}
	if err := cfg.ConfigFromFile(path); err != nil {
		panic(fmt.Sprintf("failed to load config %v", err))
	}
	return cfg
}

func NewConfigFromString(configString string) (*Config, error) {
	cfg := &Config{}
	_, err := toml.Decode(configString, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cfg, nil
}

// ParseCmd parses flag definitions from argument list
func (c *Config) ParseCmd(arguments []string) error {
	// ParseCmd first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

// ConfigFromFile loads config from file.
func (c *Config) ConfigFromFile(path string) error {
	if strings.HasSuffix(path, ".toml") {
		_, err := toml.DecodeFile(path, &c.PipelineConfig)
		if err != nil {
			return errors.Trace(err)
		}
	} else if strings.HasSuffix(path, ".json") {
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return errors.Trace(err)
		}
		err = json.Unmarshal(content, &c.PipelineConfig)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		return errors.Errorf("unrecognized path %s", path)
	}
	c.PipelineConfig.SetDefault()
	return nil
}
