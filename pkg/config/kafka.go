package config

import (
	"time"
)

type KafkaCommonConfig struct {
	ClientID          string `mapstructure:"client-id" toml:"client-id" json:"client-id"`
	ChannelBufferSize int    `mapstructure:"channel-buffer-size" toml:"channel-buffer-size" json:"channel-buffer-size"`
}

type KafkaNetConfig struct {
	// SASL based authentication with broker. While there are multiple SASL authentication methods
	// the current implementation is limited to plaintext (SASL/PLAIN) authentication
	SASL SASL `mapstructure:"sasl" toml:"sasl" json:"sasl"`

	// KeepAlive specifies the keep-alive period for an active network connection.
	// If zero, keep-alives are disabled. (default is 0: disabled).
	KeepAlive time.Duration
}

type SASL struct {
	Enable   bool   `mapstructure:"enable" toml:"enable" json:"enable"`
	User     string `mapstructure:"user" toml:"user" json:"user"`
	Password string `mapstructure:"password" toml:"password" json:"password"`
}

type Flush struct {
	Bytes       int    `mapstructure:"bytes" toml:"bytes" json:"bytes"`
	Messages    int    `mapstructure:"messages" toml:"messages" json:"messages"`
	Frequency   string `mapstructure:"frequency" toml:"frequency" json:"frequency"`
	MaxMessages int    `mapstructure:"max-messages" toml:"max-messages" json:"max-messages"`
}

type KafkaProducerConfig struct {
	Flush Flush `mapstructure:"flush" toml:"flush" json:"flush"`
}

type Fetch struct {
	Min     int32 `toml:"min" json:"min"`
	Default int32 `toml:"default" json:"default"`
	Max     int32 `toml:"max" json:"max"`
}

type Offsets struct {
	CommitInterval string `toml:"commit-interval" json:"commit-interval"`
}

type KafkaConsumerConfig struct {
	MaxWaitTime string `mapstructure:"max-wait-time" toml:"max-wait-time" json:"max-wait-time"`
	// Fetch is the namespace for controlling how many bytes are retrieved by any
	// given request.
	Fetch Fetch `mapstructure:"fetch" toml:"fetch" json:"fetch"`

	Offsets Offsets `mapstructure:"offsets" toml:"offsets" json:"offsets"`
}

type KafkaGlobalConfig struct {
	BrokerAddrs []string             `mapstructure:"broker-addrs" toml:"broker-addrs" json:"broker-addrs"`
	CertFile    string               `mapstructure:"cert-file" toml:"cert-file" json:"cert-file"`
	KeyFile     string               `mapstructure:"key-file" toml:"key-file" json:"key-file"`
	CaFile      string               `mapstructure:"ca-file" toml:"ca-file" json:"ca-file"`
	VerifySSL   bool                 `mapstructure:"verify-ssl" toml:"verify-ssl" json:"verify-ssl"`
	Mode        string               `mapstructure:"mode" toml:"mode" json:"mode"`
	Producer    *KafkaProducerConfig `mapstructure:"producer" toml:"producer" json:"producer"`
	Net         *KafkaNetConfig      `mapstructure:"net" toml:"net" json:"net"`
}
