package logic

import (
	"context"
	"fmt"
	"github.com/littlerivercc/go-sync/internal/utils"
	"github.com/zeromicro/go-zero/core/logc"
	"go.uber.org/zap"
	"math"
	"strings"

	"github.com/littlerivercc/go-sync/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

const InsertTable = "INSERT INTO %s "
const MysqlHost = "SELECT %s FROM mysql('%s:%d','%s','%s', '%s', '%s') %s;"
const CreateTable = "CREATE TABLE IF NOT EXISTS %s ENGINE = %s %s ORDER BY %s AS "

type PingLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PingLogic {
	logx.Errorf("Open gorm_sql.log failed:%v", svcCtx.Config.Core.InsertNum)
	return &PingLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (s *PingLogic) Run() error {

	for k, v := range s.svcCtx.Config.Tables {

		if v.Skip {
			fmt.Println("skip:", k, v)
			continue
		}

		tableName := v.Name
		tableNameCK := utils.GetTableNameForRand(tableName)

		tableSql := s.GetCKCreateTableSql(tableNameCK,
			v.Engine,
			v.Partition,
			fmt.Sprintf("(%s)", v.Order),
			tableName)

		err := s.Sync2CKWithWhereV2(tableName, tableNameCK, tableSql, true, true, "", "", "")
		if err != nil {
			fmt.Println(err)
			continue
		}

	}

	return nil
}

func (s *PingLogic) Sync2CKWithWhereV2(tableName, tableNameCK, tableSql string, insert, removeOld bool, whereCus, ckWhere string, newTableName string) error {
	var (
		err error
	)

	fmt.Println(fmt.Sprintf("------------------%s------------------", tableName))
	if err = s.doProcessAll(tableName, tableNameCK, tableSql, insert, whereCus, ckWhere); err != nil {

	}
	fmt.Println("-----------------------------------------------")

	fmt.Println("|-【Begin】判断生成表是否存在")
	if ok := s.svcCtx.CK.Exec(fmt.Sprintf("select * FROM %s limit 0", tableNameCK)); ok.Error != nil {
		fmt.Println("error ", ok.Error.Error())
		return ok.Error
	}

	fmt.Println("|-【Do   】交换新表和旧表")
	if len(newTableName) > 0 {
		tableName = newTableName
	}
	logc.Debug(context.Background(), "EXCHANGE")
	if ok := s.svcCtx.CK.Exec(fmt.Sprintf("EXCHANGE TABLES %s AND %s", tableNameCK, tableName)); ok.Error != nil {

		s.Logger.Error(context.Background(), "EXCHANGE", zap.Any("error", ok.Error))
		fmt.Println("|-【Do   】重命名新表")
		if ok := s.svcCtx.CK.Exec(fmt.Sprintf("RENAME TABLE %s TO %s", tableNameCK, tableName)); ok.Error != nil {
			fmt.Println("error ", ok.Error.Error())
			return ok.Error
		}
	}
	if removeOld {
		fmt.Println("|-【Do   】删除旧表")
		if ok := s.svcCtx.CK.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableNameCK)); ok.Error != nil {
			fmt.Println("error ", ok.Error.Error())
			return ok.Error
		}
	}
	fmt.Println("|-【Done 】成功")

	return err
}

func (s *PingLogic) doProcessAll(tableName, tableNameCK string, sql string, insert bool, whereCus, ckWhere string) error {
	//fmt.Println("【Begin】删除表")
	//if ok := s.svcCtx.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableNameCK)); ok.Error != nil {
	//	fmt.Println("error ", ok.Error.Error())
	//	return ok.Error
	//}
	//fmt.Println("【Done 】删除表 ")

	fmt.Println("【Begin】创建表 ", sql)
	if ok := s.svcCtx.CK.Exec(sql); ok.Error != nil {
		fmt.Println("error ", ok.Error.Error())
		return ok.Error
	}
	fmt.Println("【Done 】创建表 ")

	if !insert {
		return nil
	}

	return s.doInsertAllData(tableName, tableNameCK, whereCus, ckWhere)
}

func (s *PingLogic) doInsertAllData(tableName, tableNameCK string, whereCus, ckWhere string) error {
	var cnt int64

	if len(whereCus) > 0 {
		if ok := s.svcCtx.DB.Table(tableName).Where(strings.ReplaceAll(whereCus, "where", "")).Count(&cnt); ok.Error != nil {
			fmt.Println("error ", ok.Error.Error())
			return ok.Error
		}
	} else {
		if ok := s.svcCtx.DB.Table(tableName).Count(&cnt); ok.Error != nil {
			fmt.Println("error ", ok.Error.Error())
			return ok.Error
		}
	}

	cfg := s.svcCtx.Config
	batchCK := float64(cfg.Core.InsertNum)
	fmt.Println("|-【Do   】总数 ", cnt)
	batchTotal := int(math.Ceil(float64(cnt) / batchCK))
	fmt.Println("|-【Do   】分批次 ", batchTotal)

	where := "limit 1000000 offset %d"
	if len(whereCus) > 0 {
		where = whereCus + " " + where
	}
	if len(ckWhere) > 0 {
		where = ckWhere + " limit 1000000 offset %d"
	}
	fmt.Println("|-【Begin】插入数据 ")
	for i := 0; i < batchTotal; i++ {
		sql := s.GetCKInsertTableSql(tableNameCK, tableName, fmt.Sprintf(where, i*int(batchCK)), "*")

		fmt.Println("|-【Doing】批次 ", i+1)
		fmt.Println("|-【Doing】sql ", sql)
		if ok := s.svcCtx.CK.Exec(sql); ok.Error != nil {
			fmt.Println("error ", ok.Error.Error())
			return ok.Error
		}
		fmt.Println("|-【Done 】批次 ", i+1)
	}
	fmt.Println("|-【Done 】插入数据 ")

	return nil
}

func (s *PingLogic) GetCKCreateTableSql(tableNameCK, engine, partition, orderFiled, tableNameMySql string) string {
	cfg := s.svcCtx.Config.MySQL
	return fmt.Sprintf(CreateTable, tableNameCK, engine, partition, orderFiled) + fmt.Sprintf(MysqlHost, "*", cfg.Host, cfg.Port, cfg.Database, tableNameMySql, cfg.User, cfg.Password, "limit 0")
}

func (s *PingLogic) GetCKInsertTableSql(tableNameCK, tableNameMySql, where, columns string) string {
	cfg := s.svcCtx.Config.MySQL
	return fmt.Sprintf(InsertTable, tableNameCK) + fmt.Sprintf(MysqlHost, columns, cfg.Host, cfg.Port, cfg.Database, tableNameMySql, cfg.User, cfg.Password, where)
}
