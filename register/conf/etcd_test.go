/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-13
*/
package conf

import "testing"

func TestConf_GetDBConf(t *testing.T) {
	conf := CreateConf(&EtcdDriver{nil, 3, []string{"127.0.0.1:2379"}, "", ""})
	dbConfig := conf.GetDBConf("account")
	println(dbConfig.Write.DSN)
}
