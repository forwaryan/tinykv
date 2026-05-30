package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError 包装 kvrpcpb.KeyError，让它实现 Go 的 error 接口。
type KeyError struct {
	kvrpcpb.KeyError
}

// Error 把 protobuf KeyError 转成普通 Go error 字符串。
func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn 表示一个事务上下文。
// 它把 MVCC 里的 timestamp、write、lock 等概念封装成对底层 storage 的普通读写。
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

// NewMvccTxn 创建一个事务辅助对象。
// 它会以 startTs 为读取版本，并收集之后需要原子写入的修改。
func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes 返回当前事务已经收集到的所有底层 storage 修改。
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite 在指定 key 和 commit timestamp 下写入一条 Write 记录。
// Lab4A 会把 key 和 ts 编码后，把序列化 Write 存到 write CF。
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfWrite,
			Key:   EncodeKey(key, ts),
			Value: write.ToBytes(),
		},
	})
}

// GetLock 读取 key 上的 lock。
// 没有 lock 时返回 nil；lock 存在于 lock CF，key 使用原始 user key。
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return ParseLock(value)
}

// PutLock 给当前事务追加一个写 lock 的修改。
// server 之后会把这个修改统一写入底层 storage。
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfLock,
			Key:   key,
			Value: lock.ToBytes(),
		},
	})
}

// DeleteLock 给当前事务追加一个删除 lock 的修改。
// 提交或回滚某个 key 后，需要删除 lock CF 中的对应记录。
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfLock,
			Key: key,
		},
	})
}

// GetValue 读取在当前事务 start timestamp 可见的 key/value。
// 它需要找到 startTs 之前最近提交的 write record，再去 default CF 读取真实 value。
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for iter.Seek(EncodeKey(key, txn.StartTS)); iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.Key()

		if !bytes.Equal(DecodeUserKey(itemKey), key) {
			break
		}

		value, err := item.Value()
		if err != nil {
			return nil, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}

		switch write.Kind {
		case WriteKindPut:
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		case WriteKindDelete:
			return nil, nil
		case WriteKindRollback:
			continue
		}
	}

	return nil, nil
}

// PutValue 给当前事务追加一个写 default CF value 的修改。
// 临时 value 会用事务 start timestamp 编码保存。
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfDefault,
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
		},
	})
}

// DeleteValue 给当前事务追加一个删除 default CF value 的修改。
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfDefault,
			Key: EncodeKey(key, txn.StartTS),
		},
	})
}

// CurrentWrite 查找当前事务 start timestamp 对应的 write record。
// commit 和 rollback 会用它保证重复请求是幂等的。
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for iter.Seek(EncodeKey(key, TsMax)); iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.Key()

		if !bytes.Equal(DecodeUserKey(itemKey), key) {
			break
		}

		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(itemKey), nil
		}
	}

	return nil, 0, nil
}

// MostRecentWrite 查找某个 key 最新的一条 write record。
// Prewrite 会用它检测是否存在比当前事务 startTs 更新的写冲突。
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	iter.Seek(EncodeKey(key, TsMax))
	if !iter.Valid() {
		return nil, 0, nil
	}

	item := iter.Item()
	itemKey := item.Key()
	if !bytes.Equal(DecodeUserKey(itemKey), key) {
		return nil, 0, nil
	}

	value, err := item.Value()
	if err != nil {
		return nil, 0, err
	}

	write, err := ParseWrite(value)
	if err != nil {
		return nil, 0, err
	}

	return write, decodeTimestamp(itemKey), nil
}

// EncodeKey 把 user key 和 timestamp 编码成 MVCC 内部 key。
// 编码后会先按 user key 升序，再按 timestamp 降序排列，
// 这样扫描一个 key 时可以先遇到最新版本。
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey 从 MVCC 内部 key 中还原原始 user key。
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp 从 MVCC 内部 key 中解析 timestamp。
// 它会反转 EncodeKey 中对 timestamp 做的按位取反。
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime 返回 timestamp 中的物理时间部分。
// Lab4C 检查 lock TTL 是否过期时会用到它。
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
