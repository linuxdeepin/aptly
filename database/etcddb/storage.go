package etcddb

import (
	"fmt"
	"github.com/aptly-dev/aptly/database"
	"github.com/syndtr/goleveldb/leveldb"
	"go.etcd.io/etcd/client/v3"
)

type etcd_storage struct {
	url string
	db  *clientv3.Client
}

// CreateTemporary creates new DB of the same type in temp dir
func (s *etcd_storage) CreateTemporary() (database.Storage, error) {
	return s, nil
}

// Get key value from etcd
func (s *etcd_storage) Get(key []byte) (value []byte, err error) {
	getResp, err := s.db.Get(Ctx, string(key))
	if err != nil {
		return
	}
	for _, kv := range getResp.Kvs {
		value = kv.Value
	}
	if len(value) == 0 {
		err = database.ErrNotFound
		return
	}
	return
}

// Put saves key to etcd, if key has the same value in DB already, it is not saved
func (s *etcd_storage) Put(key []byte, value []byte) (err error) {
	_, err = s.db.Put(Ctx, string(key), string(value))
	if err != nil {
		return
	}
	return
}

// Delete removes key from etcd
func (s *etcd_storage) Delete(key []byte) (err error) {
	_, err = s.db.Delete(Ctx, string(key))
	if err != nil {
		return
	}
	return
}



// KeysByPrefix returns all keys that start with prefix
func (s *etcd_storage) KeysByPrefix(prefix []byte) [][]byte {
	fmt.Println("run KeysByPrefix", string(prefix))
	result := make([][]byte, 0, 20)
	getResp, err := s.db.Get(Ctx, string(prefix), clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		return nil
	}
	for _, ev := range getResp.Kvs {
		key := ev.Key
		keyc := make([]byte, len(key))
		copy(keyc, key)
		result = append(result, key)
	}
	return result
}

// FetchByPrefix returns all values with keys that start with prefix
func (s *etcd_storage) FetchByPrefix(prefix []byte) [][]byte {
	fmt.Println("run FetchByPrefix", string(prefix))
	result := make([][]byte, 0, 20)
	getResp, err := s.db.Get(Ctx, string(prefix), clientv3.WithPrefix())
	if err != nil {
		fmt.Println("err:", err)
		return nil
	}
	for _, kv := range getResp.Kvs{
		valc := make([]byte, len(kv.Value))
		copy(valc, kv.Value)
		result = append(result, kv.Value)
	}

	return result
}

// HasPrefix checks whether it can find any key with given prefix and returns true if one exists
func (s *etcd_storage) HasPrefix(prefix []byte) bool {
	fmt.Println("run HasPrefix", prefix)
	getResp, err := s.db.Get(Ctx, string(prefix), clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
	}
	if getResp.Count != 0 {
		return true
	}
	return false
}

// ProcessByPrefix iterates through all entries where key starts with prefix and calls
// StorageProcessor on key value pair
func (s *etcd_storage) ProcessByPrefix(prefix []byte, proc database.StorageProcessor) error {
	fmt.Println("run ProcessByPrefix", string(prefix))
	getResp, err := s.db.Get(Ctx, string(prefix), clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
	}

	for _, kv := range getResp.Kvs {
		err := proc(kv.Key, kv.Value)
		if err != nil {
			//fmt.Println("proc error: ",err)
			return err
		}
	}
	return nil
}

// Close finishes etcd connect
func (s *etcd_storage) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// Reopen tries to open (re-open) the database
func (s *etcd_storage) Open() error {
	if s.db != nil {
		return nil
	}
	var err error
	s.db, err = internalOpen(s.url)
	return err
}

// CreateBatch creates a Batch object
func (s *etcd_storage) CreateBatch() database.Batch {
	fmt.Println("run CreateBatch")
	return &etcdbatch{
		db: s.db,
		b:  &leveldb.Batch{},
	}
}

// OpenTransaction creates new transaction.
func (s *etcd_storage) OpenTransaction() (database.Transaction, error) {
	cli, err := internalOpen(s.url)
	if err != nil {
		return nil, err
	}
	kvc := clientv3.NewKV(cli)
	return &transaction{t: kvc}, nil
}

// CompactDB compacts database by merging layers
func (s *etcd_storage) CompactDB() error {
	fmt.Println("run CompactDB")
	return nil
}

// Drop removes all the etcd files (DANGEROUS!)
func (s *etcd_storage) Drop() error {
	fmt.Println("run Drop")
	return nil
}


// Check interface
var (
	_ database.Storage = &etcd_storage{}
)
