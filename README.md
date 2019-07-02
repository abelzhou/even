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
-[x] 读写分离   
-[x] 缓存
-[x] 配置中心  
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
	db := CreateDriver(NewMySQLConnector(config), nil)

```

获得一个数据或多个数据  
> Fetch 获取的结果均以map[string]interface{} 或者 []map[string]interface{} 类型返回  
> FetchOne 只会获得查询结果的第一条记录  
```go
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

    user, err := db.Prepared("SELECT * FROM users").ScanOne(&user)
    users,err := db.Prepared("SELECT * FROM users").ScanAll(&users)

```

切换主从
> 默认为主库
```go
    user, err := db.Slave().Prepared("SELECT * FROM users").FetchOne()
    users,err := db.Master().Prepared("SELECT * FROM users").FetchAll()
```

增删改
```go
    insertId,err := db.Prepared("INSERT INTO `usertest` values (null,?,?,?,?)", "18600019873", "RbTest", time.Now(), time.Now()).LastInsertID()
    affectedCount, err := db.Prepared("DELETE FROM `usertest` WHERE `id`=?", 1).AffectedCount()
```

事务
```go
    if err := db.Begin(); err != nil {
    		//
    }
    
    insertId,err := db.Prepared("INSERT INTO `usertest` values (null,?,?,?,?)", "18600019873", "RbTest", time.Now(), time.Now()).LastInsertID()
    if err!=nil || insertId==0{
    	db.Rollback()
    }
    
    //其他逻辑
    
    db.Commit()
    
    
```