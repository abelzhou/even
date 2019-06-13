/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-13
*/
package database

//Database config
type DBConfig struct {
	DSN         string
	MaxActive   int //Max active connections.
	MaxIdle     int //Max Idle connections.
	IdleTimeout int //Second
}

//Database connect config
type Config struct {
	Write          *DBConfig
	Read           []*DBConfig
	DefMaxActive   int //Default max active connections.
	DefMaxIdle     int //Default max idle connections.
	DefIdleTimeout int //Default idle timeout.Second
}


type Cache interface {
	Get(key string) interface{}
	Set(key string,value interface{}) bool
	SetWithExpire(key string, value interface{}, expire int32) bool
}