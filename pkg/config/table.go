package config

import "github.com/moiot/gravity/pkg/utils"

type TableConfig struct {
	Schema string `toml:"schema" json:"schema"`
	Table  string `toml:"table" json:"table"`

	RenameColumns map[string]string `toml:"rename-columns" json:"rename-columns"`
	IgnoreColumns []string          `toml:"ignore-columns" json:"ignore-columns"`

	PkOverride []string `toml:"pk-override" json:"pk-override"`

	ScanColumn string `toml:"scan-column" json:"scan-column"`
	ScanType   string `toml:"scan-type" json:"scan-type"`
}

func GetTableConfig(tableConfig []TableConfig, schema string, table string) *TableConfig {
	for i := range tableConfig {
		if utils.Glob(tableConfig[i].Schema, schema) && utils.Glob(tableConfig[i].Table, table) {
			return &tableConfig[i]
		}
	}
	return nil
}
