/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-10
*/
package sql

import (
	"github.com/AbelZhou/even/database"
	"log"
	"sync"
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
	conns := NewMySQLPool(config)
	db := conns.Master()
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
	conns := NewMySQLPool(config)
	db := conns.Master()
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
	conns := NewMySQLPool(config)
	db := conns.Master()
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
			t.Errorf("Delete failed.Affected count:%d userid:%d.\n", delRes, user["id"])
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
	conns := NewMySQLPool(config)
	db := conns.Master()

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

type Usertest struct {
	Id         int64
	Mobile     string
	Nickname   string
	CreateTime time.Time `db:"create_time"`
	UpdateTime time.Time `db:"update_time"`
}

func TestDBAdapter_Scan(t *testing.T) {

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
	//cacher := cache.NewMemcahce([]string{"127.0.0.1:11211"})
	conns := NewMySQLPool(config)
	db := conns.Master()

	now := time.Now()
	insertSql := "insert into `usertest` values(null,?,?,?,?)"
	id1, err := db.Prepared(insertSql, "12877717277", "scanOne", now, now).LastInsertID()

	var u1 Usertest
	if err = db.Prepared("SELECT * FROM `usertest` WHERE `id`=?", id1).ScanOne(&u1); err != nil {
		t.Error(err.Error())
	}

	if u1.Nickname == "scanOne" {
		t.Logf("Success the nickname is \"%s\",time is %s", u1.Nickname, u1.UpdateTime)
	}

	//var u2 Usertest
	//if err = db.Prepared("SELECT * FROM `usertest` WHERE `id`=?", id1).ScanOne(&u2); err != nil {
	//	t.Error(err.Error())
	//} else {
	//	t.Logf("Cache success the nickname is \"%s\",time is %s", u2.Nickname, u2.UpdateTime)
	//}

	var ulist []Usertest
	if err = db.Prepared("SELECT * FROM `usertest` LIMIT 20").ScanAll(&ulist); err != nil {
		t.Error(err.Error())
	} else {
		for i := 0; i < len(ulist); i++ {
			t.Logf("SUCCESS id:%d create_time:%s", ulist[i].Id, ulist[i].CreateTime)
		}
	}

}

var wg sync.WaitGroup

func TestDBAdapter_Goroutine(t *testing.T) {
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

	conns := NewMySQLPool(config)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t.Logf("%d \n", i)
			db := conns.Master()
			db.Begin()
			user, err := db.Prepared("SELECT * FROM `usertest` LIMIT 1").FetchOne()
			if err != nil {
				panic(err)
			}
			t.Logf("%d:%t:%v\n", i, db.inTransaction, user)
			db.Commit()
		}(i)
	}
	wg.Wait()
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
	conns := NewMySQLPool(config)
	db := conns.Master()
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
	conns := NewMySQLPool(config)
	db := conns.Master()
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
