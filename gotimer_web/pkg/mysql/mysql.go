package mysql

import (
	"errors"
	"fmt"
	mysql2 "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gotimer_web/common/conf"
)

const DuplicateEntryErrCode = 1062

type Client struct {
	*gorm.DB
}

//db 和 _db 的区别:
/*
`db` 和 `_db` 都是与数据库连接相关的变量，但它们在代码中扮演不同的角色。
`db`：
   - `db` 是使用 `gorm.Open` 函数创建的 `*gorm.DB` 类型的变量。它代表了数据库的主连接，用于执行数据库操作，如查询、插入、更新和删除等。
   - `gorm.DB` 提供了 GORM 库的所有功能，包括自动迁移、关联、预加载等。
   - `db` 变量通常用于整个应用程序的生命周期内，它是通过配置创建的，并且在整个应用程序中被重复使用。
*`_db`：
   - `_db` 是通过 `db.DB()` 方法获取的 `*sql.DB` 类型的变量。这个变量是底层的 SQL 连接池，它遵循 Go 标准库的 `sql` 包的接口。
   - `*sql.DB` 管理连接池，提供了连接的最大空闲和最大打开数量的设置。这些设置有助于控制资源使用，避免因为打开太多数据库连接而导致的资源耗尽。
   - `_db` 变量在这里仅用于设置连接池的参数，如 `SetMaxIdleConns` 和 `SetMaxOpenConns`，之后它没有被用于执行任何数据库操作。

简而言之，`db` 是 GORM 库的高级封装，提供了许多便利的数据库操作方法，而 `_db` 是 Go 标准库 `sql` 包中的底层数据库连接池，用于管理数据库连接的资源。在这段代码中，`db` 用于日常的数据库交互，而 `_db` 用于配置连接池的资源限制。
*/

func GetClient(confProvider *conf.MysqlConfProvider) (*Client, error) {
	conf := confProvider.Get()
	db, err := gorm.Open(mysql.Open(conf.DSN), &gorm.Config{})
	if err != nil {
		panic(fmt.Errorf("failed to connect databse, err : %w", err))
	}
	_db, err := db.DB()
	if err != nil {
		panic(err)
	}
	_db.SetMaxIdleConns(conf.MaxIdleConns)
	_db.SetMaxOpenConns(conf.MaxOpenConns)
	return &Client{db}, nil
}

func NewClient(db *gorm.DB) *Client {
	return &Client{db}
}

// 判断传入的错误 err 是否是一个由于尝试插入或更新数据库时违反了唯一性约束
// （如主键或唯一索引）而导致的重复项错误
func IsDuplicateEntryErr(err error) bool {
	var mysqlErr *mysql2.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == DuplicateEntryErrCode
}
