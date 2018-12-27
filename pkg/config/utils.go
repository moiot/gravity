package config

import "github.com/BurntSushi/toml"

func NewConfigFromString(configString string, c interface{}) error {
	_, err := toml.Decode(configString, c)
	return err
}
