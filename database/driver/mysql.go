package driver

import (
	"database/sql"
	goSQLDriver "github.com/go-sql-driver/mysql"
)

type EvenMySQLDriver struct {
	goSQLDriver.MySQLDriver
}

func init() {
	sql.Register("even_mysql", &EvenMySQLDriver{})
}