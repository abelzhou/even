/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package sql

import (
	"database/sql"
	"github.com/AbelZhou/even/database"
	"log"
	"math/rand"
	"time"
)

//db operator
type DBAdapter struct {
	dbConfig      *database.Config
	writer        *sql.DB
	reader        []*sql.DB
	current       *sql.DB //The current database which is operator.
	inTransaction bool
	tx            *sql.Tx
	stmt          *sql.Stmt
	preparedSql   string
	args          []interface{}
}

//create mysql sql.
func CreateMySqlDriver(config *database.Config) (db *DBAdapter) {
	return createDriver(config, "even_mysql")
}

//create sql.
func createDriver(config *database.Config, driverName string) (db *DBAdapter) {
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
	return &DBAdapter{dbConfig: config, writer: writerConn, reader: readerConn, inTransaction: false, current: writerConn}
}

//Change to slave database connection.
//All slave database connection are random
func (db *DBAdapter) Slave() (slaveDb *DBAdapter) {
	if !db.inTransaction && db.current == db.writer {
		db.current = db.reader[rand.Intn(len(db.reader))]
	}
	return db
}

//Change to master database connection.
func (db *DBAdapter) Master() (masterDb *DBAdapter) {
	db.current = db.writer
	return db
}

//Ping&Pong. Return true or false on current database connection.
//db.PING() or db.Master().PING()
func (db *DBAdapter) PING() bool {
	err := db.current.Ping()
	if err != nil {
		return false
	}
	return true
}

//begin transaction
func (db *DBAdapter) Begin() (err error) {
	//using master database and locked
	db.Master()
	db.inTransaction = true
	db.tx, err = db.current.Begin()
	if err != nil {
		return
	}

	return
}

//commit transaction
func (db *DBAdapter) Commit() (err error) {
	//unlock
	db.inTransaction = false
	//tx commit
	if db.tx != nil {
		return db.tx.Commit()
	}
	return
}

//rollback transaction
func (db *DBAdapter) Rollback() (err error) {
	//unlock
	db.inTransaction = false

	//tx rollback
	if db.tx != nil {
		return db.tx.Rollback()
	}
	return nil
}

// set prepared sql & data
func (db *DBAdapter) Prepared(sql string, args ...interface{}) *DBAdapter {
	db.preparedSql = sql
	db.args = args
	return db
}

// get first row.
// create new prepared statement object in every call.
func (db *DBAdapter) FetchOne() (res map[string]interface{}, err error) {
	defer db.clear()
	ress, err := db.query(db.preparedSql, db.args...)
	if err != nil {
		//process error
		log.Fatal()
	}
	if len(ress) == 0 {
		return nil, nil
	}
	return ress[0], nil
}

//get all rows.
func (db *DBAdapter) FetchAll() (res []map[string]interface{}, err error) {
	defer db.clear()
	res, err = db.query(db.preparedSql, db.args...)
	return res, err
}

//get last insert ID.
func (db *DBAdapter) LastInsertID() (int64, error) {
	defer db.clear()
	res, err := db.execute(db.preparedSql, db.args...)
	if err != nil {
		return 0, err
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return lastInsertID, err
}

//get affected count
func (db *DBAdapter) AffectedCount() (int64, error) {
	defer db.clear()
	res, err := db.execute(db.preparedSql, db.args...)
	if err != nil {
		return 0, err
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return lastInsertID, err
}

// clear prepared sql and args
func (db *DBAdapter) clear() {
	db.preparedSql = ""
	db.args = nil
}

//query data
func (db *DBAdapter) query(preparedSql string, args ...interface{}) (res []map[string]interface{}, err error) {
	var (
		stmt *sql.Stmt
		rows *sql.Rows
	)
	//create new statement pointer
	if db.inTransaction {
		stmt, err = db.tx.Prepare(preparedSql)
	} else {
		stmt, err = db.current.Prepare(preparedSql)
	}

	if err != nil {
		return
	}
	defer stmt.Close()

	//logic
	if len(args) == 0 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(args...)
	}

	if err != nil {
		panic(err)
		return
	}

	defer rows.Close()

	res, err = buildResultMap(rows, false)
	return
}

//execute prepared sql
func (db *DBAdapter) execute(preparedSql string, args ...interface{}) (sql.Result, error) {
	var (
		stmt *sql.Stmt
		err  error
	)
	if db.inTransaction {
		stmt, err = db.tx.Prepare(preparedSql)
	} else {
		stmt, err = db.current.Prepare(preparedSql)
	}
	if err != nil {
		return nil, err
	}

	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		return nil, err
	}

	return result, err
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

func buildResultMap(rows *sql.Rows, getFirst bool) (result []map[string]interface{}, err error) {
	var (
		columnsProp []*sql.ColumnType
	)
	columnsProp, err = rows.ColumnTypes()
	if err != nil {
		return
	}

	size := len(columnsProp)
	for rows.Next() {
		columns := make([]interface{}, size)
		columnPointers := make([]interface{}, size)
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, columnType := range columnsProp {
			val := columnPointers[i].(*interface{})
			databaseType := columnType.DatabaseTypeName()
			colName := columnType.Name()

			switch databaseType {
			case "VARCHAR":
				bv := (*val).([]byte)
				m[colName] = string(bv[:])

			case "CHAR":
				bv := (*val).([]byte)
				m[colName] = string(bv[:])

			case "TINYINT":
				m[colName] = int8((*val).(int64))

			case "SMALLINT":
				m[colName] = int16((*val).(int64))

			case "TEXT":
				bv := (*val).([]byte)
				m[colName] = string(bv[:])

			//case "DATETIME":
			//	bv := (*val).([]byte)
			//	loc, _ := time.LoadLocation("Local")
			//	m[colName], _ = time.ParseInLocation("2006-01-02 15:04:05", string(bv[:]), loc)
			//
			//case "DATE":
			//	bv := (*val).([]byte)
			//	loc, _ := time.LoadLocation("Local")
			//	m[colName], _ = time.ParseInLocation("2006-01-02 15:04:05", string(bv[:]), loc)

			default:
				m[colName] = *val
			}

			//println("trace:", columnType.Name(), databaseType, reflect.TypeOf(*val).String(), reflect.TypeOf(m[columnType.Name()]).String())
		}
		result = append(result, m)
		if getFirst {
			return
		}
	}
	return
}
