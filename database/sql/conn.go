/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package sql

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/AbelZhou/even/database"
	"log"
	"math/rand"
)


//db operator
type DBAdapter struct {
	connector     *Connector
	cacher        database.Cache
	cached        bool
	cacheExpire   int32   // default 60 seconds
	current       *sql.DB //The current database which is operator.
	inTransaction bool
	tx            *sql.Tx
	stmt          *sql.Stmt
	preparedSql   string
	args          []interface{}
}

//create sql database adapter.
func CreateDriver(connector *Connector, cacheEngine database.Cache) (db *DBAdapter) {
	return &DBAdapter{
		connector:     connector,
		inTransaction: false,
		current:       connector.writer,
		cacher:        cacheEngine,
		cached:        false,
		cacheExpire:   60,
	}
}

// Change to slave database connection.
// All slave database connection are random
func (db *DBAdapter) Slave() (slaveDb *DBAdapter) {
	if !db.inTransaction && db.current == db.connector.writer {
		db.current = db.connector.reader[rand.Intn(len(db.connector.reader))]
	}
	return db
}

// Change to master database connection.
func (db *DBAdapter) Master() (masterDb *DBAdapter) {
	db.current = db.connector.writer
	return db
}

// Query & execute with cache
func (db *DBAdapter) Cached() *DBAdapter {
	if !db.inTransaction {
		db.cached = true
	}
	return db
}

// Query & execute with cache & expire
func (db *DBAdapter) CachedWithExpire(expire int32) *DBAdapter {
	if !db.inTransaction {
		db.cached = true
		db.cacheExpire = expire
	}
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
	sql, args = db.beforePrepared(sql, args...)

	db.preparedSql = sql
	db.args = args

	db.afterPrepared()
	return db
}

// get first row.
// create new prepared statement object in every call.
func (db *DBAdapter) FetchOne() (res map[string]interface{}, err error) {
	defer db.clear()

	// get data from cacher
	cacheData := db.beforeQuery()
	if cacheData != nil {
		return cacheData[0], nil;
	}

	ress, err := db.query()
	if err != nil {
		//process error
		log.Fatal()
	}
	if len(ress) == 0 {
		return nil, nil
	}
	db.afterQuery(ress)
	return ress[0], nil
}

//get all rows.
func (db *DBAdapter) FetchAll() (res []map[string]interface{}, err error) {
	defer db.clear()

	// get data from cacher
	cacheData := db.beforeQuery()
	if cacheData != nil {
		return cacheData, nil;
	}

	res, err = db.query()
	db.afterQuery(res)
	return res, err
}

//get last insert ID.
func (db *DBAdapter) LastInsertID() (int64, error) {
	defer db.clear()

	db.beforeExecute()
	res, err := db.execute()
	if err != nil {
		return 0, err
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	db.afterExecute()
	return lastInsertID, err
}

//get affected count
func (db *DBAdapter) AffectedCount() (int64, error) {
	defer db.clear()

	db.beforeExecute()
	res, err := db.execute()
	if err != nil {
		return 0, err
	}
	affectedCount, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	db.afterExecute()
	return affectedCount, err
}

// hook begin
func (db *DBAdapter) beforePrepared(sql string, args ...interface{}) (string, []interface{}) {
	return sql, args
}

func (db *DBAdapter) afterPrepared() {

}

func (db *DBAdapter) beforeQuery() []map[string]interface{} {
	// get cache data from cacher.
	var returnData []map[string]interface{}
	if db.cached {
		keyHash := hash(append(db.args, db.preparedSql))
		key := fmt.Sprintf("%x", keyHash)
		if cacheData := db.cacher.Get(key); cacheData == nil {
			returnData = nil
		} else {
			returnData = cacheData.([]map[string]interface{})
		}
	}
	return returnData
}

func (db *DBAdapter) afterQuery(queryResult []map[string]interface{}) {
	if db.cached {
		keyHash := hash(append(db.args, db.preparedSql))
		key := fmt.Sprintf("%x", keyHash)
		_ = db.cacher.SetWithExpire(key, queryResult, db.cacheExpire)
	}
}

func (db *DBAdapter) beforeExecute() {

}

func (db *DBAdapter) afterExecute() {

}

func hash(arr []interface{}) [16]byte {
	var arrBytes []byte
	for _, item := range arr {
		jsonBytes, _ := json.Marshal(item)
		arrBytes = append(arrBytes, jsonBytes...)
	}
	return md5.Sum(arrBytes)
}

// hook end

// clear prepared sql and args
func (db *DBAdapter) clear() {
	db.preparedSql = ""
	db.args = nil
	db.cached = false
	db.cacheExpire = 60
}

//query data
func (db *DBAdapter) query() (res []map[string]interface{}, err error) {
	if db.preparedSql == "" {
		return nil, ERR_NOPREPARED
	}

	var (
		stmt *sql.Stmt
		rows *sql.Rows
	)
	//create new statement pointer
	if db.inTransaction {
		stmt, err = db.tx.Prepare(db.preparedSql)
	} else {
		stmt, err = db.current.Prepare(db.preparedSql)
	}

	if err != nil {
		return
	}
	defer stmt.Close()

	//logic
	if len(db.args) == 0 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(db.args...)
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
func (db *DBAdapter) execute() (sql.Result, error) {
	if db.preparedSql == "" {
		return nil, ERR_NOPREPARED
	}

	var (
		stmt *sql.Stmt
		err  error
	)
	if db.inTransaction {
		stmt, err = db.tx.Prepare(db.preparedSql)
	} else {
		stmt, err = db.current.Prepare(db.preparedSql)
	}
	if err != nil {
		return nil, err
	}

	defer stmt.Close()
	result, err := stmt.Exec(db.args...)
	if err != nil {
		return nil, err
	}

	return result, err
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
