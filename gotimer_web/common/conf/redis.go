package conf

type RedisConfig struct {
	Network            string `yaml:"net_work"`
	Address            string `yaml:"address"`
	Password           string `yaml:"password"`
	MaxIdle            int    `yaml:"maxIdle"`
	IdleTimeoutSeconds int    `yaml:"idleTimeoutSeconds"`
	// 连接池的最大存活数
	MaxActive int `yaml:"maxActive"`
	// 当连接数达到上限，新的请求是等待还是立即报错
	Wait bool `yaml:"wait"`
}

type RedisConfigProvider struct {
	conf *RedisConfig
}

func NewRedisConfigProvider(congf *RedisConfig) *RedisConfigProvider {
	return &RedisConfigProvider{
		conf: congf,
	}
}

func (r *RedisConfigProvider) Get() *RedisConfig { return r.conf }

var defaultRedisConfProvider *RedisConfigProvider

func DefaultRedisConfigProvider() *RedisConfigProvider { return defaultRedisConfProvider }
