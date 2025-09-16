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
	lk := &Lock{ck: ck, lkey: l, lsta: kvtest.RandValue(8), ver: 0}
	// _, ver, gerr := lk.ck.Get(l)
	// if gerr == rpc.ErrNoKey {
	// 	perr := lk.ck.Put(lk.lkey, "", ver)
	// 	if perr != rpc.OK {
	// 		log.Printf("MakeLock Bug, putRpcErr: %s\n", perr)
	// 	}
	// 	// lk.ver = ver + 1
	// } else {
	// 	// lk.ver = 1
	// }
	return lk
}

func (lk *Lock) Acquire() {
	for lsta, ver, gerr := lk.ck.Get(lk.lkey); ; lsta, ver, gerr = lk.ck.Get(lk.lkey) {
		if gerr != rpc.OK && gerr != rpc.ErrNoKey {
			log.Fatalf("[%s] Lock acquire Bug, getRpcErr: %s", lk.lsta, gerr)
		} else if gerr == rpc.ErrNoKey || (gerr == rpc.OK && len(lsta) == 0) { // lock free
			lk.ver = ver
			perr := lk.ck.Put(lk.lkey, lk.lsta, lk.ver)
			if perr == rpc.OK { // acquire fail
				lk.ver = ver + 1 // acquire success
				break
			} else if perr == rpc.ErrMaybe {
				lsta, ver, gerr = lk.ck.Get(lk.lkey)
				if gerr == rpc.OK && ver == lk.ver+1 && lsta == lk.lsta {
					lk.ver = ver
					break
				}
			} else {
				// log.Printf("[%s] Acquire lock fail\n", lk.lsta)
			}
		} else if lsta == lk.lsta { // already acquired
			// log.Printf("[%s] ReAquire lock\n", lk.lsta)
			// time.Sleep(time.Millisecond * 500)
			break
		} else if len(lsta) > 0 { // locked retry
			lk.ver = ver
			continue
		}
	}
}

func (lk *Lock) Release() {
	// if lsta, ver, gerr := lk.ck.Get(lk.lkey); gerr != rpc.OK {
	// 	log.Fatalf("[%s] Lock Release Bug, getRpcErr: %s", lk.lsta, gerr)
	// } else if lsta == lk.lsta {
	// 	// lk.ver = ver
	// 	for perr := lk.ck.Put(lk.lkey, "", ver); !putvVerOK(perr); perr = lk.ck.Put(lk.lkey, "", ver) {
	// 		log.Printf("[%s] release mayfail, retry\n", lk.lsta)
	// 	}
	// 	// if perr != rpc.OK && perr != rpc.ErrMaybe {
	// 	// 	log.Fatalf("[%s] Lock Release Bug, putRpcErr: %s\n", lk.lsta, perr)
	// 	// }
	// } else if lsta != lk.lsta {
	// 	// lk.ver = ver
	// 	// log.Fatalf("[%s] Lock Release Bug, sta not samed, respsta: %s\n", lk.lsta, lsta)
	// } else {
	// 	log.Fatalln("Not Known Bug")
	// }
	if perr := lk.ck.Put(lk.lkey, "", lk.ver); putvVerOK(perr) {
		lk.ver += 1
	} else {
		lsta, ver, gerr := lk.ck.Get(lk.lkey)
		if ver != lk.ver {
			log.Fatalf("[%s] Lock Release Bug, lockVer: %d, getSta: [%s], getVer: %d, getErr: %s", lk.lsta, lk.ver, lsta, ver, gerr)
		}
	}
}

func putvVerOK(perr rpc.Err) bool {
	return perr == rpc.OK || perr == rpc.ErrMaybe
}
