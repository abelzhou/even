/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package driver

import (
	"database/sql"
	goSQLDriver "github.com/go-sql-driver/mysql"
)

type EvenMySqlDriver struct {
	goSQLDriver.MySQLDriver
}

func init() {
	sql.Register("even_mysql", &EvenMySqlDriver{})
}