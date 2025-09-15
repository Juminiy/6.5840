package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	lkey string
	lsta string
	ver  rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lkey: kvtest.RandValue(8), lsta: l, ver: 0}
	_, ver, gerr := lk.ck.Get(l)
	if gerr == rpc.ErrNoKey {
		perr := lk.ck.Put(lk.lkey, "", ver)
		if perr != rpc.OK {
			log.Fatalf("MakeLock Bug, putRpcErr: %s", perr)
		}
		lk.ver += 1
	} else {
		lk.ver = ver
	}
	return lk
}

func (lk *Lock) Acquire() {
	for lsta, ver, gerr := lk.ck.Get(lk.lkey); ; {
		if gerr != rpc.OK {
			log.Fatalf("Lock Acquire Bug, getRpcErr: %s", gerr)
		} else if lsta == lk.lsta { // locked retry
			lk.ver = ver
			continue
		} else if len(lsta) == 0 { // lock free
			lk.ver = ver
			lk.ck.Put(lk.lkey, lk.lsta, ver)
			break
		}
	}
}

func (lk *Lock) Release() {
	if lsta, ver, gerr := lk.ck.Get(lk.lkey); gerr != rpc.OK {
		log.Fatalf("Lock Release Bug, getRpcErr: %s", gerr)
	} else if lsta == lk.lsta {
		lk.ver = ver
		lk.ck.Put(lk.lkey, "", ver)
	} else {
		lk.ver = ver
	}
}
