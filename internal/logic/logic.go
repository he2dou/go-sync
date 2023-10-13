package logic

import (
	"context"
	"fmt"
	"github.com/littlerivercc/go-sync/internal/config"
	"github.com/littlerivercc/go-sync/internal/svc"
	"github.com/littlerivercc/go-sync/internal/utils"
	"github.com/zeromicro/go-zero/core/logc"
	"go.uber.org/zap"
	"math"

	"github.com/zeromicro/go-zero/core/logx"
)

const InsertTableSql = "INSERT INTO %s "
const CreateTableSql = "CREATE TABLE IF NOT EXISTS %s ENGINE = %s %s ORDER BY %s AS "

const MysqlHost = "SELECT %s FROM mysql('%s:%d','%s','%s', '%s', '%s') %s;"

type PingLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	tables []string
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

func (s *PingLogic) Run(tables []string) error {

	for _, v := range s.svcCtx.Config.Tables {
		if v.Skip {
			fmt.Println("skip", v.Name)
			continue
		}
		if len(tables) > 0 && !utils.Contains(tables, v.Name) {
			fmt.Println("contains", v.Name)
			continue
		}
		err := s.Sync(v)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func (s *PingLogic) Sync(v config.Table) error {
	var (
		err error
	)

	tableName := v.Name
	tableNameCK := utils.GetTableNameForRand(tableName)

	// 创建表
	err = s.CreateTable(tableNameCK, v.Engine, v.Partition, fmt.Sprintf("(%s)", v.Order), tableName)
	if err != nil {
		return err
	}

	if !v.Insert {
		return nil
	}

	// 插入表
	err = s.InsertTable(tableName, tableNameCK, v.Where)
	if err != nil {
		return err
	}

	// 交换表
	return s.Exchange(tableName, tableNameCK, v.Rename)
}

func (s *PingLogic) Exchange(tableName, tableNameCK, rename string) error {
	fmt.Println("|-【Do   】交换新表和旧表")
	if len(rename) > 0 {
		tableName = rename
	}
	db := s.svcCtx.CK
	logc.Debug(context.Background(), "EXCHANGE")
	if ok := db.Exec(fmt.Sprintf("EXCHANGE TABLES %s AND %s", tableNameCK, tableName)); ok.Error != nil {
		s.Logger.Error(context.Background(), "EXCHANGE", zap.Any("error", ok.Error))
		fmt.Println("|-【Do   】重命名新表")
		if ok := db.Exec(fmt.Sprintf("RENAME TABLE %s TO %s", tableNameCK, tableName)); ok.Error != nil {
			fmt.Println("error ", ok.Error.Error())
			return ok.Error
		}
	}
	fmt.Println("|-【Do   】删除旧表")
	if ok := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableNameCK)); ok.Error != nil {
		fmt.Println("error ", ok.Error.Error())
		return ok.Error
	}
	fmt.Println("|-【Done 】成功")
	return nil
}

func (s *PingLogic) InsertTable(tableName, tableNameCK string, whereCus string) error {

	var cnt int64
	cfg := s.svcCtx.Config
	db := s.svcCtx.DB.Table(tableName)
	if len(whereCus) > 0 {
		db = db.Where(whereCus)
	}

	if ok := db.Count(&cnt); ok.Error != nil {
		fmt.Println("error ", ok.Error.Error())
		return ok.Error
	}

	batchCK := cfg.Core.InsertNum
	fmt.Println("|-【Do   】总数 ", cnt)
	batchTotal := int(math.Ceil(float64(cnt) / float64(batchCK)))
	fmt.Println("|-【Do   】分批次 ", batchTotal)

	where := "limit %d offset %d"
	if len(whereCus) > 0 {
		where = " where " + whereCus + " " + where
	}

	fmt.Println("|-【Begin】插入数据 ")
	for i := 0; i < batchTotal; i++ {
		w := fmt.Sprintf(where, batchCK, i*batchCK)
		sql := s.GetInsertTableSql(tableNameCK, tableName, w)

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

func (s *PingLogic) CreateTable(tableNameCK, engine, partition, orderFiled, tableName string) error {
	sql := s.GetCreateTableSql(tableNameCK, engine, partition, orderFiled, tableName)
	fmt.Println("【Begin】创建表 ", sql)
	if ok := s.svcCtx.CK.Exec(sql); ok.Error != nil {
		fmt.Println("error ", ok.Error.Error())
		return ok.Error
	}
	fmt.Println("【Done 】创建表 ")
	return nil
}

func (s *PingLogic) GetCreateTableSql(tableNameCK, engine, partition, orderFiled, tableName string) string {
	cfg := s.svcCtx.Config.MySQL
	return fmt.Sprintf(CreateTableSql, tableNameCK, engine, partition, orderFiled) +
		fmt.Sprintf(MysqlHost, "*", cfg.Host, cfg.Port, cfg.Database, tableName, cfg.User, cfg.Password, "limit 0")
}

func (s *PingLogic) GetInsertTableSql(tableNameCK, tableName, where string) string {
	cfg := s.svcCtx.Config.MySQL
	return fmt.Sprintf(InsertTableSql, tableNameCK) +
		fmt.Sprintf(MysqlHost, "*", cfg.Host, cfg.Port, cfg.Database, tableName, cfg.User, cfg.Password, where)
}
