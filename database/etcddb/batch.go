package etcddb

import (
	"encoding/binary"
	"github.com/aptly-dev/aptly/database"
	"github.com/aws/aws-sdk-go/aws/client"
)

type keyType uint

type batchIndex struct {
	keyType            keyType
	keyPos, keyLen     int
	valuePos, valueLen int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

func (index batchIndex) kv(data []byte) (key, value []byte) {
	return index.k(data), index.v(data)
}

type Batch struct {
	data  []byte
	index []batchIndex

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen int
}

type batch struct {
	db *client.Client
	b  *Batch
}

const (
	keyTypeDel = keyType(0)
	keyTypeVal = keyType(1)
	batchHeaderLen = 8 + 4
	batchGrowRec   = 3000
	batchBufioSize = 16
)

func (b *Batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n {
		div := 1
		if len(b.index) > batchGrowRec {
			div = len(b.index) / batchGrowRec
		}
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}


func (b *Batch) appendRec(kt keyType, key, value []byte) {
	n := 1 + binary.MaxVarintLen32 + len(key)
	if kt == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	index := batchIndex{keyType: kt}
	o := len(b.data)
	data := b.data[:o+n]
	data[o] = byte(kt)
	o++
	o += binary.PutUvarint(data[o:], uint64(len(key)))
	index.keyPos = o
	index.keyLen = len(key)
	o += copy(data[o:], key)
	if kt == keyTypeVal {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	b.index = append(b.index, index)
	b.internalLen += index.keyLen + index.valueLen + 8
}

func (b *Batch) Put(key, value []byte) {
	b.appendRec(keyTypeVal, key, value)
}

func (b *Batch) Delete(key []byte) {
	b.appendRec(keyTypeDel, key, nil)
}

func (b *batch) Put(key, value []byte) (err error) {
	b.b.Put(key, value)

	return
}

func (b *Batch) Dump() []byte {
	return b.data
}

func (b *batch) Delete(key []byte) (err error) {
	b.b.Delete(key)
	return
}

func (b *batch) Write() (err error) {
	return
}

// batch should implement database.Batch
var (
	_ database.Batch = &batch{}
)
