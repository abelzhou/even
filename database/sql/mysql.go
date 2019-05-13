/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package sql

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
)

type EvenMySqlDriver struct {
	mysql.MySQLDriver
}

func init() {
	sql.Register("even_mysql", &EvenMySqlDriver{})
}