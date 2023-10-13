package svc

import (
	"fmt"
	"github.com/littlerivercc/go-sync/internal/config"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

const (
	DbtypeMysql      = "mysql"
	DbtypeClickhouse = "clickhouse"
)

func InitDB(c config.Config) (*gorm.DB, func(), error) {
	return NewDB(DbtypeMysql, c)
}

func NewDB(dbType string, c config.Config) (*gorm.DB, func(), error) {
	var dia gorm.Dialector
	if dbType == DbtypeMysql {
		dia = mysql.New(mysql.Config{
			DSN: c.MySQL.DSN(),
		})
	} else if dbType == DbtypeClickhouse {
		dia = clickhouse.Open(c.ClickHouse.DSN())
	}

	db, err := gorm.Open(dia, &gorm.Config{
		//Logger: logger.New(log.New(f, "\r\n", log.LstdFlags), logger.Config{
		//	SlowThreshold: time.Second, // 慢SQL阈值
		//	LogLevel:      logger.Info, // 日志级别
		//	Colorful:      true,        // 颜色打印
		//}),
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   c.Gorm.TablePrefix,
			SingularTable: true,
		},
	})
	if err != nil {
		return nil, nil, err
	}
	if c.Gorm.Debug {
		db = db.Debug()

		//f, err := os.OpenFile("logs/gorm_sql.log", os.O_APPEND|os.O_WRONLY, 6)
		//if err != nil {
		//	logx.Errorf("Open gorm_sql.log failed:%v", err)
		//	return nil, nil, err
		//}
		//
		//db.Logger = logger.New(log.New(f, "\r\n", log.LstdFlags), logger.Config{
		//	SlowThreshold: time.Second, // 慢SQL阈值
		//	LogLevel:      logger.Info, // 日志级别
		//	Colorful:      true,        // 颜色打印
		//})
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	cleanFunc := func() {
		err := sqlDB.Close()
		if err != nil {
			fmt.Sprintf("Gorm db close error: %s", err.Error())
		}
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, cleanFunc, err
	}
	sqlDB.SetMaxIdleConns(c.Gorm.MaxIdleConns)
	sqlDB.SetMaxOpenConns(c.Gorm.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(time.Duration(c.Gorm.MaxLifetime) * time.Second)

	return db, cleanFunc, nil
}

func InitCK(c config.Config) (*gorm.DB, func(), error) {
	return NewDB(DbtypeClickhouse, c)
}
