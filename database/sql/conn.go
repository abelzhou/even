/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type Conn struct {
	isReader      bool
	db            *sql.DB
	inTransaction bool
	tx            *sql.Tx
	stmt          *sql.Stmt
	preparedSql   string
	args          []interface{}
}

//Ping&Pong. Return true or false on current database connection.
func (conn *Conn) PING() bool {
	err := conn.db.Ping()
	if err != nil {
		return false
	}
	return true
}

//begin transaction
func (conn *Conn) Begin() (err error) {
	if conn.isReader {
		return ERR_READERTRANSACTION
	}
	conn.inTransaction = true
	conn.tx, err = conn.db.Begin()
	if err != nil {
		return
	}

	return
}

//commit transaction
func (conn *Conn) Commit() (err error) {
	//unlock
	conn.inTransaction = false
	//tx commit
	if conn.tx != nil {
		return conn.tx.Commit()
	}
	return
}

//rollback transaction
func (conn *Conn) Rollback() (err error) {
	//unlock
	conn.inTransaction = false

	//tx rollback
	if conn.tx != nil {
		return conn.tx.Rollback()
	}
	return nil
}

// set prepared sql & data
func (conn *Conn) Prepared(sql string, args ...interface{}) *Conn {
	sql, args = conn.beforePrepared(sql, args...)

	conn.preparedSql = sql
	conn.args = args

	conn.afterPrepared()
	return conn
}

// get first row.
// create new prepared statement object in every call.
func (conn *Conn) FetchOne() (res map[string]interface{}, err error) {
	defer conn.clear()

	// get data from cacher
	cacheData := conn.beforeQuery()
	if cacheData != nil {
		data := cacheData.([]map[string]interface{})
		return data[0], nil
	}

	rows, err := conn.query()
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
	conn.afterQuery(ress)
	return ress[0], nil
}

// get all rows.
func (conn *Conn) FetchAll() (res []map[string]interface{}, err error) {
	defer conn.clear()

	// get data from cacher
	cacheData := conn.beforeQuery()
	if cacheData != nil {
		data := cacheData.([]map[string]interface{})
		return data, nil
	}

	rows, err := conn.query()
	if err != nil {
		return nil, err
	}

	res, err = buildResultMap(rows, false)
	if err != nil {
		return nil, err
	}

	conn.afterQuery(res)
	return res, err
}

// get one raw to a struct.
// no cache
func (conn *Conn) ScanOne(v interface{}) error {
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
	defer conn.clear()

	//cacheData := db.beforeQuery()
	//if cacheData != nil {
	//	v = cacheData
	//	return nil
	//}

	//query
	rows, err := conn.query()
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
func (conn *Conn) ScanAll(out interface{}) error {
	// defer database clear
	defer conn.clear()
	rows, err := conn.query()
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
func (conn *Conn) LastInsertID() (int64, error) {
	defer conn.clear()

	conn.beforeExecute()
	res, err := conn.execute()
	if err != nil {
		return 0, err
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	conn.afterExecute()
	return lastInsertID, err
}

//get affected count
func (conn *Conn) AffectedCount() (int64, error) {
	defer conn.clear()

	conn.beforeExecute()
	res, err := conn.execute()
	if err != nil {
		return 0, err
	}
	affectedCount, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	conn.afterExecute()
	return affectedCount, err
}

// hook begin
func (conn *Conn) beforePrepared(sql string, args ...interface{}) (string, []interface{}) {
	return sql, args
}

func (conn *Conn) afterPrepared() {

}

func (conn *Conn) beforeQuery() interface{} {
	// get cache data from cacher.
	return nil
}

func (conn *Conn) afterQuery(queryResult interface{}) {
}

func (conn *Conn) beforeExecute() {

}

func (conn *Conn) afterExecute() {

}

// hook end

// clear prepared sql and args
func (conn *Conn) clear() {
	conn.preparedSql = ""
	conn.args = nil
	// After rows object have been closed.We must close prepared statement
	if conn.stmt != nil {
		conn.stmt.Close()
	}
}

//query data
func (conn *Conn) query() (sqlRows *sql.Rows, err error) {
	if conn.preparedSql == "" {
		return nil, ERR_NOPREPARED
	}

	var (
		rows *sql.Rows
	)
	//create new statement pointer
	if conn.inTransaction {
		conn.stmt, err = conn.tx.Prepare(conn.preparedSql)
	} else {
		conn.stmt, err = conn.db.Prepare(conn.preparedSql)
	}

	if err != nil {
		return
	}

	//logic
	if len(conn.args) == 0 {
		rows, err = conn.stmt.Query()
	} else {
		rows, err = conn.stmt.Query(conn.args...)
	}

	if err != nil {
		return
	}

	return rows, nil
}

//execute prepared sql
func (conn *Conn) execute() (sql.Result, error) {
	if conn.preparedSql == "" {
		return nil, ERR_NOPREPARED
	}

	var (
		err error
	)
	if conn.inTransaction {
		conn.stmt, err = conn.tx.Prepare(conn.preparedSql)
	} else {
		conn.stmt, err = conn.db.Prepare(conn.preparedSql)
	}
	if err != nil {
		return nil, err
	}

	result, err := conn.stmt.Exec(conn.args...)
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
		for i := range columns {
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
			default:
				m[colName] = *val
			}

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
