package raft

import (
	"log"
	"math/rand"
	"os"
	"time"
)

// Debugging
const Debug = true

func init() {
	logfd, _ := os.Create("log/srv.log")
	log.SetOutput(logfd)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// [rgL, rgR]
func timeDurMs(rgL, rgR int64) time.Duration {
	ms := rgL
	if rgR > rgL {
		ms = rgL + (rand.Int63() % (rgR - rgL))
	}
	return time.Duration(ms) * time.Millisecond
}
