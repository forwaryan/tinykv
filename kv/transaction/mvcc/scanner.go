package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner 用来从 storage 层连续读取多个 MVCC key/value。
// 它理解 MVCC 的内部编码，并向用户返回去重后的可见版本。
type Scanner struct {
	// txn 提供当前 scan 的读取版本和底层 reader。
	txn *MvccTxn
	// iter 顺序扫描 write CF；write CF 决定哪些 user key 在当前版本可见。
	iter engine_util.DBIterator
}

// NewScanner 创建一个从 startKey 开始扫描的 MVCC scanner。
// Lab4C 会在这里初始化迭代器，让 Next 能按顺序返回可见 user key。
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	// 从 startKey 在当前读版本能看到的第一条 write record 开始扫描。
	iter.Seek(EncodeKey(startKey, txn.StartTS))

	return &Scanner{
		txn:  txn,
		iter: iter,
	}
}

// Close 释放 scanner 持有的迭代器资源。
func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next 返回下一组可见 key/value。
// 如果扫描结束，返回 nil, nil, nil；它需要跳过旧版本和删除版本，
// 并保证每个 user key 最多返回一次。
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for scan.iter.Valid() {
		item := scan.iter.Item()
		itemKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(itemKey)

		if decodeTimestamp(itemKey) > scan.txn.StartTS {
			// 可能是从上一个 key 跳到更大的 user key 时落在了未来版本；先跳到该 key 的读版本。
			scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
			continue
		}

		// write CF 中同一个 user key 的多个版本连续排列，且 timestamp 从新到旧。
		for scan.iter.Valid() {
			item = scan.iter.Item()
			itemKey = item.KeyCopy(nil)

			// 当前 user key 的所有可见候选版本都处理完了，交给外层循环处理下一个 key。
			if !bytes.Equal(DecodeUserKey(itemKey), userKey) {
				break
			}

			value, err := item.Value()
			if err != nil {
				return nil, nil, err
			}

			write, err := ParseWrite(value)
			if err != nil {
				return nil, nil, err
			}

			switch write.Kind {
			case WriteKindPut:
				// write record 只保存 start_ts；真正 value 在 default CF 的 key@start_ts。
				result, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
				if err != nil {
					return nil, nil, err
				}
				// 当前 user key 已经返回，下一次 scan 从严格更大的 user key 继续。
				nextKey := append(append([]byte{}, userKey...), 0)
				scan.iter.Seek(EncodeKey(nextKey, scan.txn.StartTS))
				return userKey, result, nil

			case WriteKindDelete:
				// 当前版本下这个 key 已删除，跳过整个 user key。
				nextKey := append(append([]byte{}, userKey...), 0)
				scan.iter.Seek(EncodeKey(nextKey, scan.txn.StartTS))
				break

			case WriteKindRollback:
				// rollback record 不代表可见值，继续看同一个 key 的更老版本。
				scan.iter.Next()
			}
		}
	}

	return nil, nil, nil
}
