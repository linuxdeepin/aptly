package etcddb

import (
	"fmt"
	"github.com/aptly-dev/aptly/database"
	"github.com/syndtr/goleveldb/leveldb"
	"go.etcd.io/etcd/client/v3"
)

type etcdbatch struct {
	db *clientv3.Client
	b  *leveldb.Batch
}

type WriteOptions struct {
	NoWriteMerge bool
	Sync bool
}

func (b *etcdbatch) Put(key, value []byte) error {
	fmt.Println("etcd batch Put")
	b.b.Put(key, value)
	return nil
}

func (b *etcdbatch) Delete(key []byte) error {
	fmt.Println("etcd batch Delete")
	b.b.Delete(key)
	return nil
}

func (b *etcdbatch) Write() error {
	fmt.Println("etcd batch Write")
	return Write(b.b, &WriteOptions{})
}

// batch should implement database.Batch
var (
	_ database.Batch = &etcdbatch{}
)
