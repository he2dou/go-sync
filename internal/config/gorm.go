package config

import "fmt"

type Gorm struct {
	TablePrefix  string
	Debug        bool
	MaxLifetime  int
	MaxOpenConns int
	MaxIdleConns int
}

type ClickHouse struct {
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	ReadTimeout  int
	WriteTimeout int
}

func (m ClickHouse) DSN() string {
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d",
		m.Host, m.Port, m.User, m.Password, m.Database, m.ReadTimeout, m.WriteTimeout)
}

type MySQL struct {
	Host       string
	Port       int
	User       string
	Password   string
	Database   string
	Parameters string
}

func (a MySQL) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		a.User, a.Password, a.Host, a.Port, a.Database, a.Parameters)
}
