/**
 *  author:Abel
 *  email:abel.zhou@hotmail.com
 *  date:2019-05-28
 */
package cache

import (
	"github.com/bradfitz/gomemcache/memcache"
	json "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack"
	"reflect"
)

const (
	flagBool    = 1
	flagFloat64 = 2
	flagString  = 3
	flagMap     = 4
	flagArray   = 5
)

type Memcache struct {
	gomc *memcache.Client
}

func NewMemcahce(server []string) *Memcache {
	client := memcache.New(server...)
	return &Memcache{gomc: client}
}

func (m *Memcache) Get(key string) interface{} {
	item, err := m.gomc.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return nil
		} else {
			panic(err)
		}
		return nil
	}

	if item.Flags == 0 {
		return nil
	}
	var v interface{}


	if err := msgpack.Unmarshal(item.Value, v); err != nil {
		return nil
	}

	//  convert to []map[string]interface from []interface
	if item.Flags == flagArray {
		//fmt.Printf("%x \n",v)
		sliceObj := v.([]interface{})
		//check type for map[string]interface
		if reflect.TypeOf(sliceObj[0]) == reflect.TypeOf(map[string]interface{}{}) {
			result := make([]map[string]interface{}, len(sliceObj))
			for i := 0; i < len(sliceObj); i++ {
				result[i] = sliceObj[i].(map[string]interface{})
			}
			return result
		}
	}
	return v
}

func (m *Memcache) Set(key string, value interface{}) bool {
	b, err := msgpack.Marshal(value)
	//b, err := json.Marshal(value)
	if err != nil {
		return false
	}
	flag := m.getFlag(value)
	if flag == 0 {
		return false
	}

	if err := m.gomc.Set(&memcache.Item{Key: key, Value: b, Flags: flag}); err != nil {
		return false
	}
	return true
}

func (m *Memcache) SetWithExpire(key string, value interface{}, expire int32) bool {
	b, err := json.Marshal(value)
	if err != nil {
		return false
	}
	flag := m.getFlag(value)
	if flag == 0 {
		return false
	}

	if err := m.gomc.Set(&memcache.Item{Key: key, Value: b, Expiration: expire, Flags: flag}); err != nil {
		return false
	}
	return true
}

func (m *Memcache) getFlag(value interface{}) uint32 {
	t := reflect.TypeOf(value)
	switch t.Kind() {
	case reflect.Slice:
		return flagArray
	case reflect.String:
		return flagString
	case reflect.Map:
		return flagMap
	case reflect.Bool:
		return flagBool
	default:
		var i64 float64
		if t.ConvertibleTo(reflect.TypeOf(i64)) {
			return flagFloat64
		}
	}
	return 0
}
//
//func (m *Memcache) getStructRef(flag uint32) interface{} {
//	switch flag {
//	case flagMap:
//		return make(map[string]interface{})
//	case flagArray:
//		return make([]interface{}, 0)
//	case flagString:
//		var s string
//		return &s
//	case flagBool:
//		var s bool
//		return &s
//	case flagFloat64:
//		var i int64
//		return &i
//	}
//	return nil
//}
