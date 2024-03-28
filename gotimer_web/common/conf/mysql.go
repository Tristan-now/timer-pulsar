package conf

type MySQLConfig struct {
	DSN string `yaml:"DSN"`
	// 最大连接数
	MaxOpenConns int `yaml:"MaxOpenConns"`
	// 最大空闲连接数(连接池中已经建立但并没有使用的连接)
	MaxIdleConns int `yaml:"MaxIdleConns"`
}

type MysqlConfProvider struct {
	conf *MySQLConfig
}

func NewMysqlConfigProvider(conf *MySQLConfig) *MysqlConfProvider {
	return &MysqlConfProvider{
		conf: conf,
	}
}

func (m *MysqlConfProvider) Get() *MySQLConfig {
	return m.conf
}

var defaultMysqlConfProvider *MysqlConfProvider

func DefaultMysqlConfigProvider() *MysqlConfProvider {
	return defaultMysqlConfProvider
}
