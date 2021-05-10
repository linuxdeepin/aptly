package etcddb

import (
	"context"
	"github.com/aptly-dev/aptly/database"
	"go.etcd.io/etcd/client/v3"
	"time"
)

var Ctx = context.TODO()

func internalOpen(url string) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints: []string{url},
		DialTimeout: 5 * time.Second,
	}

	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func NewDB(url string) (database.Storage, error) {
	cli, err := internalOpen(url)
	if err != nil {
		return nil, err
	}
	return &etcd_storage{url, cli}, nil
}


func NewOpenDB(url string) (database.Storage, error) {
	db, err := NewDB(url)
	if err != nil {
		return nil, err
	}

	return db, nil
}
