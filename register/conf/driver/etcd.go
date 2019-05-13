/*
   author:Abel
   email:abel.zhou@hotmail.com
   date:2019-05-13
*/
package driver

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type EtcdDriver struct {
	client *clientv3.Client
}

// create etcd driver.
// endpoints example: []string{"localhost:2379", "localhost:22379", "localhost:32379"}
// DialTimeout is second.
func CreateEtcdDriver(endpoints []string, dialTimeout int, username string, password string) *EtcdDriver {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Duration(dialTimeout) * time.Second,
		Username:    username,
		Password:    password,
	})
	if err != nil {
		panic(err)
	}

	return &EtcdDriver{client: cli}
}

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

func (ed *EtcdDriver) Close() {
	_ = ed.client.Close()
}
