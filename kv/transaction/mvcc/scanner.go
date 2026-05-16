package mvcc

// Scanner 用来从 storage 层连续读取多个 MVCC key/value。
// 它理解 MVCC 的内部编码，并向用户返回去重后的可见版本。
type Scanner struct {
	// Your Data Here (4C).
}

// NewScanner 创建一个从 startKey 开始扫描的 MVCC scanner。
// Lab4C 会在这里初始化迭代器，让 Next 能按顺序返回可见 user key。
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return nil
}

// Close 释放 scanner 持有的迭代器资源。
func (scan *Scanner) Close() {
	// Your Code Here (4C).
}

// Next 返回下一组可见 key/value。
// 如果扫描结束，返回 nil, nil, nil；它需要跳过旧版本和删除版本，
// 并保证每个 user key 最多返回一次。
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	return nil, nil, nil
}
