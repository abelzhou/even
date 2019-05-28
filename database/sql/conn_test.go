/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package sql

import (
	"github.com/AbelZhou/even/database"
	"github.com/AbelZhou/even/database/cache"
	"log"
	"testing"
	"time"
)

/**
CREATE TABLE `usertest` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `mobile` varchar(11) NOT NULL DEFAULT '',
  `nickname` varchar(10) NOT NULL DEFAULT '',
  `create_time` datetime NOT NULL,
  `update_time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4
*/

func TestCreateMySQLDriver(t *testing.T) {
	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	db := CreateMySqlDriver(config, nil)
	res, err := db.Prepared("SELECT * FROM usertest").FetchOne()
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("%s\n", res["nickname"])
}

func TestDBAdapter_FetchAll(t *testing.T) {
	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	db := CreateMySqlDriver(config, nil)
	res, err := db.Prepared("SELECT * FROM `usertest`").FetchAll()
	if err != nil {
		log.Fatal(err)
	}
	if len(res) != 0 {
		t.Logf("NickName Test:%s  Result count:%d\n", res[0]["nickname"], len(res))
	} else {
		t.Logf("No record\n")
	}
}

func TestDBAdapter_Excute(t *testing.T) {

	insertSql := "insert into `usertest` values(null,?,?,?,?)"

	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	db := CreateMySqlDriver(config, nil)
	now := time.Now()
	id, err := db.Prepared(insertSql, "12877717278", "abc", now, now).LastInsertID()
	if err != nil {
		panic(err)
	}
	t.Logf("Last insertID %d\n", id)

	user, err := db.Prepared("SELECT * FROM `usertest`").FetchOne()
	if err != nil {
		panic(err)
	}

	if user != nil {
		t.Logf("Find user:id[%d] name[%s]\n", user["id"], user["nickname"])
		delRes, err := db.Prepared("DELETE FROM `usertest` WHERE `id`=?", user["id"]).AffectedCount()
		if err != nil {
			panic(err)
		}
		if delRes == 1 {
			t.Logf("Delete success.Affected count:%d.\n", delRes)
		} else {
			t.Logf("Delete failed.Affected count:%d.\n", delRes)
		}
	}
}

func TestDBAdapter_TransactionRollback(t *testing.T) {
	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	db := CreateMySqlDriver(config, nil)

	if err := db.Begin(); err != nil {
		panic(err)
	}

	t.Logf("In transaction:%t \n", db.inTransaction)
	insertId, err := db.Prepared("INSERT INTO `usertest` values (null,?,?,?,?)", "18600019873", "RbTest", time.Now(), time.Now()).LastInsertID()
	if err != nil {
		db.Rollback()
		panic(err)
	} else {
		t.Logf("Insert user:id[%d] \n", insertId)
	}
	user, err := db.Prepared("SELECT * FROM `usertest` WHERE `id`=?", insertId).FetchOne()
	if err != nil {
		db.Rollback()
		panic(user)
	} else {
		if user != nil && user["id"] == insertId {
			t.Logf("Insert check success in transcation. User id is %d", user["id"])
		} else {
			t.Errorf("Insert check failed in transcation. User object is nil.Or user id not %d.", user["id"])
		}
	}

	err = db.Rollback()
	if err != nil {
		t.Errorf("Rollback failed.")
	}

	checkUser, err := db.Prepared("SELECT * FROM `usertest` WHERE `id`=?", insertId).FetchOne()
	if err != nil {
		panic(err)
	}
	if checkUser != nil {
		t.Errorf("Rollback failed after rollback operation.")
	} else {
		t.Log("Rollback success.")
	}
}

func TestDBAdapter_Cached(t *testing.T) {
	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	cacher := cache.NewGCache(1024)
	db := CreateMySqlDriver(config, cacher)

	now := time.Now()
	insertSql := "insert into `usertest` values(null,?,?,?,?)"
	id1, err := db.Prepared(insertSql, "12877717277", "abc", now, now).LastInsertID()

	user, err := db.Cached().Prepared("SELECT * FROM `usertest` WHERE `id`=?", id1).FetchOne()
	if err != nil {
		panic(err)
	}
	if user["id"].(int64) == id1 {
		t.Logf("select success.UID:%d", user["id"])
	}

	user, err = db.Cached().Prepared("SELECT * FROM `usertest` WHERE `id`=?", 60103).FetchOne()
	if err != nil {
		panic(err)
	}
	if user["id"].(int64) == id1 {
		t.Logf("select success.UID:%d", user["id"])
	}

	id2, err := db.Prepared(insertSql, "12877717279", "abc", now, now).LastInsertID()
	//other data
	user, err = db.Cached().Prepared("SELECT * FROM `usertest` WHERE `id`=?", id2).FetchOne()
	if err != nil {
		panic(err)
	}
	if user["id"].(int64) == id2 {
		t.Logf("select success.UID:%d", user["id"])
	}

	user, err = db.Cached().Prepared("SELECT * FROM `usertest` WHERE `id`=?", 60104).FetchOne()
	if err != nil {
		panic(err)
	}
	if user["id"].(int64) == id2 {
		t.Logf("select success.UID:%d", user["id"])
	}

}

func BenchmarkCreateMySQLDriver(b *testing.B) {
	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	db := CreateMySqlDriver(config, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Prepared("SELECT * FROM usertest").FetchAll()
		if err != nil {
			log.Fatal(err)
		}
	}

}

func BenchmarkDBAdapter_Insert(b *testing.B) {
	var config = &database.Config{
		Write: &database.DBConfig{
			DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
		},
		Read: []*database.DBConfig{
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
			{
				DSN: "abel:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true&loc=Local",
			},
		},
		DefIdleTimeout: 200,
		DefMaxIdle:     10,
		DefMaxActive:   20,
	}
	db := CreateMySqlDriver(config, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		insertId, err := db.Prepared("INSERT INTO `usertest` values (null,?,?,?,?)", "18600019873", "BmTest", time.Now(), time.Now()).LastInsertID()
		if err != nil {
			panic(err)
		} else {
			b.Logf("Insert user:id[%d] \n", insertId)
		}
	}
}
