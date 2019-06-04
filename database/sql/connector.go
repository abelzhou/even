/**
 *  author:Abel
 *  email:abel.zhou@hotmail.com
 *  date:2019-06-04
 */
package sql

import (
	"database/sql"
	"github.com/AbelZhou/even/database"
	"time"
)

type Connector struct {
	dbConfig *database.Config
	writer   *sql.DB
	reader   []*sql.DB
}

func NewMySQLConnector(config *database.Config) *Connector{
	return NewConnecter(config,"even_mysql");
}

func NewConnecter(config *database.Config, driverName string) *Connector {
	//format database config
	configFormat(config)

	if config.Read == nil {
		config.Read[0] = config.Write
	}

	//load writer database connections
	var writerConn, err = sql.Open(driverName, config.Write.DSN)
	if err != nil {
		panic(err)
	}

	err = writerConn.Ping()
	if err != nil {
		panic(err)
	}
	writerConn.SetMaxOpenConns(config.Write.MaxActive)
	writerConn.SetMaxIdleConns(config.Write.MaxIdle)
	writerConn.SetConnMaxLifetime(time.Duration(config.Write.IdleTimeout) * time.Second)

	// load reader database connections.
	var readerConn []*sql.DB
	for _, readerConf := range config.Read {
		reader, err := sql.Open(driverName, readerConf.DSN)
		if err != nil {
			panic(err)
		}
		err = reader.Ping()
		if err != nil {
			panic(err)
		}
		reader.SetConnMaxLifetime(time.Duration(readerConf.IdleTimeout) * time.Second)
		reader.SetMaxIdleConns(readerConf.MaxIdle)
		reader.SetMaxOpenConns(readerConf.MaxActive)
		readerConn = append(readerConn, reader)
	}

	return &Connector{
		dbConfig: config,
		writer:   writerConn,
		reader:   readerConn,
	}
}

//Progress the database config.
func configFormat(dbConfig *database.Config) {
	if dbConfig.Write.MaxActive == 0 {
		dbConfig.Write.MaxActive = dbConfig.DefMaxActive
	}
	if dbConfig.Write.MaxIdle == 0 {
		dbConfig.Write.MaxIdle = dbConfig.DefMaxIdle
	}
	if dbConfig.Write.IdleTimeout == 0 {
		dbConfig.Write.IdleTimeout = dbConfig.DefIdleTimeout
	}

	for i := 0; i < len(dbConfig.Read); i++ {
		if dbConfig.Read[i].MaxActive == 0 {
			dbConfig.Read[i].MaxActive = dbConfig.DefMaxActive
		}
		if dbConfig.Read[i].MaxIdle == 0 {
			dbConfig.Read[i].MaxIdle = dbConfig.DefMaxIdle
		}
		if dbConfig.Read[i].IdleTimeout == 0 {
			dbConfig.Read[i].IdleTimeout = dbConfig.DefIdleTimeout
		}
	}
}
