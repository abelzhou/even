# database 数据库操作包  

```shell
├── conf.go
├── kv
│   ├── redis
│   └── rocks
└── sql
    ├── conn.go  //数据操作
    ├── conn_test.go 
    ├── connector.go //数据连接对象
    ├── err.go //错误const
    └── mysql.go //mysql driver
```  


## 这是一个数据库处理包  
起初是有orm的设定，但因为在实际项目操作的过程中，orm不具备给DBA输出sql的能力所以放弃了。
