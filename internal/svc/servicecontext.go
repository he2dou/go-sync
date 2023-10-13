package svc

import (
	"fmt"
	"github.com/littlerivercc/go-sync/internal/config"
	"gorm.io/gorm"
	"log"
)

type ServiceContext struct {
	Config config.Config
	DB     *gorm.DB
	CK     *gorm.DB
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, cleanup, err := InitDB(c)
	if err != nil {
		log.Fatal(err)
		cleanup()
	}
	fmt.Println("mysql connected!")
	ck, cleanup2, err := InitCK(c)
	if err != nil {
		log.Fatal(err)
		cleanup2()
	}
	fmt.Println("clickhouse connected!")
	return &ServiceContext{
		Config: c,
		DB:     db,
		CK:     ck,
	}
}
