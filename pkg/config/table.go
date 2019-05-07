package config

type TableConfig struct {
	Schema string `toml:"schema" json:"schema"`
	Table  string `toml:"table" json:"table"`

	RenameColumns map[string]string `toml:"rename-columns" json:"rename-columns"`
	IgnoreColumns []string          `toml:"ignore-columns" json:"ignore-columns"`

	PkOverride []string `toml:"pk-override" json:"pk-override"`

	ScanColumn string `toml:"scan-column" json:"scan-column"`
	ScanType   string `toml:"scan-type" json:"scan-type"`
}
