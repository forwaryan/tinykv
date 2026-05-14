package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

type standaloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db = engine_util.CreateDB(s.conf.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (r *standaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (r *standaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *standaloneStorageReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneStorageReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, modify := range batch {
		switch data := modify.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	return wb.WriteToDB(s.db)
}
