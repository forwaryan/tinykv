package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage 是单机版 TinyKV 的 Storage 实现。
// 它不和其他节点通信，所有数据都直接保存在本地 Badger 里。
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

type standaloneStorageReader struct {
	txn *badger.Txn
}

// NewStandAloneStorage 创建 Lab1 单机存储对象。
// 这里先保存配置，真正打开 Badger DB 的动作放在 Start 里做。
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
	}
}

// Start 打开 standalone 模式使用的本地 Badger 数据库。
// Lab1 的写入会直接进入这个 DB，不经过 Raft。
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db = engine_util.CreateDB(s.conf.DBPath, false)
	return nil
}

// Stop 在服务关闭时释放本地 Badger 数据库。
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// GetCF 通过当前读事务，从指定 CF 里读取一个 key。
// 如果 Badger 返回 key 不存在，这里转成 nil，方便 RawGet 设置 NotFound。
func (r *standaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

// IterCF 为指定 CF 创建迭代器，RawScan 会用它顺序扫描数据。
func (r *standaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

// Close 释放 reader 持有的 Badger 读事务。
func (r *standaloneStorageReader) Close() {
	r.txn.Discard()
}

// Reader 创建一个只读视图，用来读取当前 Badger 状态。
// RawGet 和 RawScan 会使用返回的 reader，用完后必须 Close。
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneStorageReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

// Write 把一批 Put/Delete 修改写入 Badger。
// 在 Lab1 里这是最终写入路径；到 Lab2 后，类似写入会在 Raft command 提交后才发生。
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
