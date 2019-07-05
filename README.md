# even业务框架

## 结构
```bash
├── cache #缓存
├── database #数据库
│   ├── kv #redis
│   └── sql # mysql/oracle
└── register #注册中心
    └── conf #配置中心
```


## 特性
DB  
-[x] 支持Map[string]interface{}数据返回方式  
-[x] 支持Scan方式数据返回  

Cache  
-[x] Memcache  
-[x] Gcache  

配置中心  
-[x] etcd  

服务中心  
-[ ] 服务发现  



## 数据库使用
Create Driver
```go
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
    

```

获得一个数据或多个数据  
> Fetch 获取的结果均以map[string]interface{} 或者 []map[string]interface{} 类型返回  
> FetchOne 只会获得查询结果的第一条记录  
```go
    db := conns.Slave() //获得读库链接 
    user, err := db.Prepared("SELECT * FROM users").FetchOne()
    users,err = db.Prepared("SELECT * FROM users").FetchAll()
```  


获得一个数据或者多个数据映射到对象中  
```go
    type Users struct {
    	Id         int64
    	Mobile     string
    	Nickname   string
    	CreateTime time.Time `db:"create_time"`
    	UpdateTime time.Time `db:"update_time"`
    }

    var user Users
    var users []Users

    db := conns.Slave()
    user, err := db.Prepared("SELECT * FROM users").ScanOne(&user)
    users,err := db.Prepared("SELECT * FROM users").ScanAll(&users)

```

增删改
```go
    db := conns.Master()
    insertId,err := db.Prepared("INSERT INTO `usertest` values (null,?,?,?,?)", "18600000000", "RbTest", time.Now(), time.Now()).LastInsertID()
    affectedCount, err := db.Prepared("DELETE FROM `usertest` WHERE `id`=?", 1).AffectedCount()
```

事务  
> 只有写库才能开启事务
```go
    db := conns.Master()
    if err := db.Begin(); err != nil {
    		//
    }
    
    insertId,err := db.Prepared("INSERT INTO `usertest` values (null,?,?,?,?)", "18600000000", "RbTest", time.Now(), time.Now()).LastInsertID()
    if err!=nil || insertId==0{
    	db.Rollback()
    }
    
    //其他逻辑
    
    db.Commit()
    
    
```
