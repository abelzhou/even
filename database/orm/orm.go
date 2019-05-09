package orm

import (
	"github.com/jinzhu/gorm"
	"time"
)

type DBConf struct {
	DNS          string
	MaxOpenConns int //最大活跃链接数
	MaxIdleConns int //允许的最大空闲链接数
	IdleTimeout  time.Duration //超时时间
}

func init(){
}

//type ormLog struct {}

func CreateMysql(conf *DBConf) (db *gorm.DB) {
	db, _ = gorm.Open("mysql", conf.DNS)
	db.DB().SetMaxIdleConns(conf.MaxIdleConns)
	db.DB().SetMaxOpenConns(conf.MaxOpenConns)
	db.DB().SetConnMaxLifetime(conf.IdleTimeout)
	//db.SetLogger(ormLog{})
	return
}