/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-13
*/
package conf

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"time"
)

// endpoints example: []string{"localhost:2379", "localhost:22379", "localhost:32379"}
// DialTimeout is second.
type EtcdDriver struct {
	client      *clientv3.Client
	DialTimeout int
	Endpoints   []string
	Username    string
	Password    string
}

//open conn
func (ed *EtcdDriver) Open() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   ed.Endpoints,
		DialTimeout: time.Duration(ed.DialTimeout) * time.Second,
		Username:    ed.Username,
		Password:    ed.Password,
	})
	if err != nil {
		panic(err)
	}

	ed.client = cli
}

//read conf
func (ed *EtcdDriver) Read(key string) string {
	response, err := ed.client.Get(context.TODO(), key)
	if err != nil {
		return ""
	}
	if len(response.Kvs) == 0 {
		return ""
	}
	return string(response.Kvs[0].Value)
}

//close resource.
func (ed *EtcdDriver) Close() {
	_ = ed.client.Close()
}
