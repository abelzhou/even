/**
 *  author:Abel
 *  email:abel.zhou@hotmail.com
 *  date:2019-05-27
 */
package cache

import (
	"github.com/AbelZhou/even/database"
	"github.com/bluele/gcache"
	"time"
)

type GCache struct {
	gc gcache.Cache
}

// get a new GCache with LRU.It's applied to monolithic application
func NewGCache(size int) database.Cache {
	cache := gcache.New(size).LRU().Build()
	return &GCache{gc: cache}
}

// get something
func (c *GCache) Get(key string) interface{} {
	res, err := c.gc.Get(key)
	if err != nil {
		return nil
	}
	return res
}

// set somethinge
func (c *GCache) Set(key string, value interface{}) bool {
	err := c.gc.Set(key, value)
	if err != nil {
		return false
	}
	return true
}

// set something with expire
func (c *GCache) SetWithExpire(key string, value interface{}, expire time.Duration) bool {
	err := c.gc.SetWithExpire(key, value, expire)
	if err != nil {
		return false
	}
	return true
}
