/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-13
*/
package conf

import "testing"

func TestConf_GetDBConf(t *testing.T) {
	conf := CreateConf(&EtcdDriver{Endpoints: []string{"127.0.0.1:2379"}, DialTimeout: 3, Username: "", Password: ""})
	dbConfig := conf.GetDBConf("account")
	if dbConfig.Write.DSN==""{
		t.Error("Get database config failed!")
	}
}
