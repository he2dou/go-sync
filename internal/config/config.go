package config

import "github.com/zeromicro/go-zero/core/logx"

type Config struct {
	//Mysql struct {
	//	DataSource  string
	//	AutoMigrate bool
	//}
	//ClickHouse struct {
	//	DataSource string
	//}

	Core struct {
		InsertNum int
	}

	Gorm       Gorm
	ClickHouse ClickHouse
	MySQL      MySQL
	Log        logx.LogConf
	Tables     []Table
}

type Table struct {
	//- Name: "mb_opr_account"
	Name string
	//Skip: false
	Skip bool
	//Full: true
	Full bool
	//Engine: "ReplacingMergeTree"
	Engine string
	//PartitionBy: ""  # Table create partitioning by, like toYYYYMM(created_at).
	Partition string
	//OrderBy: "id"
	Order  string
	Where  string
	Insert bool
	Rename string
}
