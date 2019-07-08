# even业务框架

## 结构
```bash
├── cache #缓存
├── database #数据库
│   ├── kv #redis
│   └── sql # mysql
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


## Quick Start
* [数据库使用](database/README.md)

