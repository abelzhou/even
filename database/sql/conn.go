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
	"math/rand"
	"reflect"
	"strings"
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
		data := cacheData.([]map[string]interface{})
		return data[0], nil
	}

	rows, err := db.query()
	if err != nil {
		//process error
		return nil, err
	}

	ress, err := buildResultMap(rows, true)
	if err != nil {
		return nil, err
	}

	if len(ress) == 0 {
		return nil, nil
	}
	db.afterQuery(ress)
	return ress[0], nil
}

// get all rows.
func (db *DBAdapter) FetchAll() (res []map[string]interface{}, err error) {
	defer db.clear()

	// get data from cacher
	cacheData := db.beforeQuery()
	if cacheData != nil {
		data := cacheData.([]map[string]interface{})
		return data, nil
	}

	rows, err := db.query()
	if err != nil {
		return nil, err
	}

	res, err = buildResultMap(rows, false)
	if err != nil {
		return nil, err
	}

	db.afterQuery(res)
	return res, err
}

// get one raw to a struct.
// no cache
func (db *DBAdapter) ScanOne(v interface{}) error {
	// check v
	vType := reflect.TypeOf(v)
	if k := vType.Kind(); k != reflect.Ptr {
		return ERR_MUSTBEPOINTER
	}

	vType = vType.Elem()
	vVal := reflect.ValueOf(v).Elem()
	if vType.Kind() == reflect.Slice {
		return ERR_MUSTNOTBESLICE
	}

	// defer database clear
	defer db.clear()

	//cacheData := db.beforeQuery()
	//if cacheData != nil {
	//	v = cacheData
	//	return nil
	//}

	//query
	rows, err := db.query()
	if err != nil {
		return err
	}

	// fill obj
	sl := reflect.New(reflect.SliceOf(vType))
	if err = fillRows(sl.Interface(), rows); err != nil {
		return err
	}
	sl = sl.Elem()

	if sl.Len() == 0 {
		return nil
	}

	vVal.Set(sl.Index(0))

	//db.afterQuery(sl)
	//time.Sleep(2000*time.Second)
	return nil
}

// get one raw to a struct slice
func (db *DBAdapter) ScanAll(out interface{}) error {
	// defer database clear
	defer db.clear()
	rows, err := db.query()
	if err != nil {
		return err
	}

	// fill obj
	if err = fillRows(out, rows); err != nil {
		return err
	}

	return nil
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

func (db *DBAdapter) beforeQuery() interface{} {
	// get cache data from cacher.
	if db.cached {
		keyHash := hash(append(db.args, db.preparedSql))
		key := fmt.Sprintf("%x", keyHash)
		if cacheData := db.cacher.Get(key); cacheData != nil {
			return cacheData
		}
	}
	return nil
}

func (db *DBAdapter) afterQuery(queryResult interface{}) {
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
	// After rows object have been closed.We must close prepared statement
	if db.stmt != nil {
		db.stmt.Close()
	}
}

//query data
func (db *DBAdapter) query() (sqlRows *sql.Rows, err error) {
	if db.preparedSql == "" {
		return nil, ERR_NOPREPARED
	}

	var (
		rows *sql.Rows
	)
	//create new statement pointer
	if db.inTransaction {
		db.stmt, err = db.tx.Prepare(db.preparedSql)
	} else {
		db.stmt, err = db.current.Prepare(db.preparedSql)
	}

	if err != nil {
		return
	}

	//logic
	if len(db.args) == 0 {
		rows, err = db.stmt.Query()
	} else {
		rows, err = db.stmt.Query(db.args...)
	}

	if err != nil {
		return
	}

	return rows, nil
}

//execute prepared sql
func (db *DBAdapter) execute() (sql.Result, error) {
	if db.preparedSql == "" {
		return nil, ERR_NOPREPARED
	}

	var (
		err error
	)
	if db.inTransaction {
		db.stmt, err = db.tx.Prepare(db.preparedSql)
	} else {
		db.stmt, err = db.current.Prepare(db.preparedSql)
	}
	if err != nil {
		return nil, err
	}

	result, err := db.stmt.Exec(db.args...)
	if err != nil {
		return nil, err
	}

	return result, err
}

func buildResultMap(rows *sql.Rows, getFirst bool) (result []map[string]interface{}, err error) {
	defer rows.Close()
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

// from https://github.com/blockloop/scan
// reflect struct
func fillRows(v interface{}, rows *sql.Rows) error {
	defer rows.Close()

	vType := reflect.TypeOf(v)
	if k := vType.Kind(); k != reflect.Ptr {
		return fmt.Errorf("%q must be a pointer", k.String())
	}
	sliceType := vType.Elem()
	if reflect.Slice != sliceType.Kind() {
		return fmt.Errorf("%q must be a slice", sliceType.String())
	}

	sliceVal := reflect.Indirect(reflect.ValueOf(v))
	itemType := sliceType.Elem()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	isPrimitive := itemType.Kind() != reflect.Struct

	for rows.Next() {
		sliceItem := reflect.New(itemType).Elem()

		var pointers []interface{}
		if isPrimitive {
			if len(cols) > 1 {
				return ERR_TOOMANEYCOLUMNS
			}
			pointers = []interface{}{sliceItem.Addr().Interface()}
		} else {
			pointers = structPointers(sliceItem, cols, false)
		}

		if len(pointers) == 0 {
			return nil
		}

		err := rows.Scan(pointers...)
		if err != nil {
			return err
		}
		sliceVal.Set(reflect.Append(sliceVal, sliceItem))
	}
	return rows.Err()
}

func structPointers(stct reflect.Value, cols []string, strict bool) []interface{} {
	pointers := make([]interface{}, 0, len(cols))

	fieldTag := initFieldTag(stct, len(cols))
	for _, colName := range cols {
		var fieldVal reflect.Value
		if v, ok := fieldTag[colName]; ok {
			fieldVal = v
		} else {
			if strict {
				fieldVal = reflect.ValueOf(nil)
			} else {
				fieldVal = stct.FieldByName(strings.Title(colName))
			}
		}
		//fieldVal := fieldByName(stct, colName, strict)
		if !fieldVal.IsValid() || !fieldVal.CanSet() {
			// have to add if we found a column because Scan() requires
			// len(cols) arguments or it will error. This way we can scan to
			// a useless pointer
			var nothing interface{}
			pointers = append(pointers, &nothing)
			continue
		}

		pointers = append(pointers, fieldVal.Addr().Interface())
	}
	return pointers
}

// Initialization the tags from struct.
func initFieldTag(v reflect.Value, len int) map[string]reflect.Value {
	fieldTagMap := make(map[string]reflect.Value, len)
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		tag, ok := typ.Field(i).Tag.Lookup("db")
		if ok && tag != "" {
			fieldTagMap[tag] = v.Field(i)
		}
	}
	return fieldTagMap
}
