package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter engine_util.DBIterator
	txn  *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, TsMax))
	return &Scanner{
		iter: iter,
		txn:  txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	si := scan.iter
	for {
		// exhausted
		if !si.Valid() {
			return nil, nil, nil
		}
		item := si.Item()
		commitTs, usrKey := decodeKey(item.Key())

		// This write was committed after the txn started.
		// Iter should skip it.
		if commitTs >= scan.txn.StartTS {
			si.Seek(EncodeKey(usrKey, commitTs-1))
			continue
		}

		// check whether key is locked
		keyError, _, err := scan.txn.CheckKeyLocked(usrKey)
		if err != nil {
			return nil, nil, err
		}
		if keyError != nil {
			return nil, nil, keyError
		}

		buf, err := item.Value()
		if err != nil {
			return nil, nil, err
		}

		write, err := ParseWrite(buf)
		if err != nil {
			return nil, nil, err
		}

		if write.Kind != WriteKindPut {
			// This key is deleted.
			si.Seek(EncodeKey(usrKey, 0))
			continue
		}

		val, err := scan.txn.Reader.GetCF(engine_util.CfDefault,
			EncodeKey(usrKey, write.StartTS))
		if err != nil {
			return nil, nil, err
		}
		scan.iter.Next()
		return usrKey, val, nil
	}

}
