/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-13
*/
package conf

import (
	"fmt"
	"github.com/AbelZhou/even/database"
	"strconv"
)

type ConfigDriver interface {
	Read(key string) string
	Close()
}

type Conf struct {
	driver ConfigDriver
}

//create conf object
func CreateConf(driver ConfigDriver) *Conf {
	return &Conf{driver: driver}
}

// get db config obj
//
// example:
// dbconf/(dbtag)/DefMaxActive 20
// dbconf/(dbtag)/DefMaxIdle 10
// dbconf/(dbtag)/DefIdleTimeout 2000
// dbconf/(dbtag)/write/DSN "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local"
// dbconf/(dbtag)/write/MaxActive 20
// dbconf/(dbtag)/write/MaxIdle 5
// dbconf/(dbtag)/write/IdleTimeout 1000
// dbconf/(dbtag)/read0/DSN "abel:123456@tcp(127.0.0.1:3307)/test?charset=utf8mb4&parseTime=true&loc=Local"
// dbconf/(dbtag)/read1/DSN "abel:123456@tcp(127.0.0.1:3308)/test?charset=utf8mb4&parseTime=true&loc=Local"
// dbconf/(dbtag)/read2/DSN "abel:123456@tcp(127.0.0.1:3309)/test?charset=utf8mb4&parseTime=true&loc=Local"
//
// TOML:
//	[account]
//    DefMaxActive = 20
//    DefMaxIdle = 10
//    DefIdleTimeout = 200
//    [write]
//        dsn = "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local"
//    [[read]]
//        dns = "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local"
//    [[read]]
//        dns = "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local"
func (c *Conf) GetDBConf(dbtag string) *database.Config {
	defer c.driver.Close()
	//load default value
	defMaxActive := 10
	defMaxIdle := 5
	defIdleTimeout := 300
	prefix := "/dbconf" + "/" + dbtag

	defMaxActive, _ = strconv.Atoi(c.driver.Read(prefix + "/DefMaxActive"))
	defMaxIdle, _ = strconv.Atoi(c.driver.Read(prefix + "/DefMaxIdle"))
	defIdleTimeout, _ = strconv.Atoi(c.driver.Read(prefix + "/DefIdleTimeout"))

	if defMaxIdle == 0 {
		defMaxIdle = 5
	}
	if defMaxActive == 0 {
		defMaxActive = 10
	}
	if defIdleTimeout == 0 {
		defIdleTimeout = 300
	}

	//get write database config.
	writeDsn := c.driver.Read(prefix + "/write/DSN")
	writeMaxActive, _ := strconv.Atoi(c.driver.Read(prefix + "/write/MaxActive"))
	wirteMaxIdle, _ := strconv.Atoi(c.driver.Read(prefix + "/write/MaxIdle"))
	wirteIdleTimeout, _ := strconv.Atoi(c.driver.Read(prefix + "/write/IdleTimeout"))

	//get read databases.
	var readers []*database.DBConfig
	idx := 0
	for {
		readerName := fmt.Sprintf("read%d", idx)
		readDsn := c.driver.Read(prefix + "/" + readerName + "/DSN")
		if readDsn == "" {
			break
		}
		readMaxActive, _ := strconv.Atoi(c.driver.Read(prefix + "/" + readerName + "/MaxActive"))
		readMaxIdle, _ := strconv.Atoi(c.driver.Read(prefix + "/" + readerName + "/MaxIdle"))
		readIdleTimeout, _ := strconv.Atoi(c.driver.Read(prefix + "/" + readerName + "/IdleTimeout"))
		readers = append(readers, &database.DBConfig{
			DSN:         readDsn,
			MaxIdle:     readMaxIdle,
			MaxActive:   readMaxActive,
			IdleTimeout: readIdleTimeout,
		})
		idx++
	}

	dbConf := &database.Config{
		DefMaxActive:   defMaxActive,
		DefMaxIdle:     defMaxIdle,
		DefIdleTimeout: defIdleTimeout,
		Write: &database.DBConfig{
			DSN:         writeDsn,
			MaxIdle:     wirteMaxIdle,
			MaxActive:   writeMaxActive,
			IdleTimeout: wirteIdleTimeout,
		},
		Read: readers,
	}
	return dbConf
}
