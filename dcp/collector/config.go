package collector

type Config interface {
	foo()
}

type MysqlConfig struct {
	Db         DbConfig
	TagConfigs []TagConfig
}

func (MysqlConfig) foo() {
}

type DbConfig struct {
	Name     string `toml:"name" json:"name"`
	Host     string `toml:"host" json:"host"`
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
	Port     uint   `toml:"port" json:"port"`
	ServerId uint32
}

type TagConfig struct {
	Tag    string
	Tables []SchemaAndTable
}

type SchemaAndTable struct {
	Schema        string
	Table         string
	PrimaryKeyIdx int
}

type GrpcConfig struct {
	Port int
}

func (GrpcConfig) foo() {
}
