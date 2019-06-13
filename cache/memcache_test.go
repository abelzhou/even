/**
 *  author:Abel
 *  email:abel.zhou@hotmail.com
 *  date:2019-05-29
 */
package cache

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type student struct {
	name     string
	age      int
	birthday time.Duration
	classNo  string
}

func TestMemcache_Set(t *testing.T) {
	t.Log("TEST [slice map]")
	var list []map[string]interface{}
	m1 := make(map[string]interface{})
	m2 := make(map[string]interface{})
	m1["name"] = "zhangsan"
	m1["age"] = "lisi"
	m1["birthday"] = time.Now()
	m2["name"] = "zhangsan"
	m2["age"] = "lisi"
	m2["birthday"] = time.Now()
	list = append(list, m1)
	list = append(list, m2)

	mc := NewMemcahce([]string{"127.0.0.1:11211"})
	mc.Set("test", list)
	res := mc.Get("test")
	if res == nil {
		t.Logf("Slice map failed %x", res)
		t.Fatal("Res [slice map] is nil.")
	} else {
		t.Logf("Slice map success %s \n", reflect.TypeOf(res))
		b := reflect.TypeOf(res).ConvertibleTo(reflect.TypeOf([]map[string]interface{}{}))
		t.Logf("%t \n\n", b)
	}

	t.Log("TEST [map]")
	mc.Set("testmap", m1)
	res = mc.Get("testmap")
	if res == nil {
		t.Logf("%x", res)
		t.Fatal("Res [map] is nil.")
	} else {
		fmt.Printf("%s \n\n", reflect.TypeOf(res))
	}
}
