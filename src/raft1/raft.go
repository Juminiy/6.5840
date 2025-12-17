package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"golang.org/x/sync/errgroup"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentRole   Role
	currentLeader int
	currentTerm   int64
	votedFor      map[int64]int // Term -> CandidateId
	log           []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	latest time.Time
}

type Role int

const (
	None      Role = 0
	Leader    Role = 1
	Candidate Role = 2
	Follower  Role = 3
)

type LogEntry struct {
	Term    int64
	Index   int
	Command any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return int(rf.currentTerm), rf.currentRole == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.currentRole = Follower
		rf.currentLeader = -1
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.latest = time.Now()
	} else { // args.Term == rf.currentTerm
		switch rf.currentRole {
		case Follower:
			rf.currentLeader = -1
			if currentVote, ok := rf.votedFor[args.Term]; !ok || currentVote == args.CandidateId {
				reply.VoteGranted = true
				rf.latest = time.Now()
			}
		case Candidate:

		case Leader:

		}
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int64
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term
	rf.currentLeader = args.LeaderId
	rf.currentRole = Follower
	rf.currentTerm = args.Term
	rf.latest = time.Now()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	for rf.currentRole != Leader {
		rf.mu.Lock()
		rf.currentRole = Candidate
		rf.currentLeader = -1
		rf.currentTerm += 1
		rf.votedFor[rf.currentTerm] = rf.me
		curTerm := rf.currentTerm
		rf.mu.Unlock()

		var voteCnt int64
		var latestTerm int64

		eg, _ := errgroup.WithContext(context.Background())
		for peerIdx := range rf.peers {
			if peerIdx != rf.me {
				eg.Go(func() error {
					args, reply := RequestVoteArgs{
						Term:        curTerm,
						CandidateId: rf.me,
					}, RequestVoteReply{}
					if rf.sendRequestVote(peerIdx, &args, &reply) {
						if reply.VoteGranted {
							atomic.AddInt64(&voteCnt, 1)
						} else {
							if reply.Term > atomic.LoadInt64(&latestTerm) {
								atomic.StoreInt64(&latestTerm, int64(reply.Term))
							}
						}
					}
					return nil
				})
			}
		}
		eg.Wait()

		if voteCnt+1 > int64(len(rf.peers)/2) { // win
			rf.mu.Lock()
			rf.currentLeader = rf.me
			rf.currentRole = Leader
			rf.mu.Unlock()
			go rf.heartBeat()
			break
		} else if curTerm < latestTerm { // latest
			rf.mu.Lock()
			rf.currentRole = Follower
			rf.currentTerm = latestTerm
			rf.currentLeader = -1
			rf.mu.Unlock()
			break
		}

	}
}

func (rf *Raft) heartBeat() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	for rf.currentRole == Leader {
		rf.mu.Lock()
		curTerm := rf.currentTerm
		rf.mu.Unlock()

		var latestTerm int64

		eg, _ := errgroup.WithContext(context.Background())
		for peerIdx := range rf.peers {
			if peerIdx != rf.me {
				eg.Go(func() error {
					args, reply := AppendEntriesArgs{
						Term:     curTerm,
						LeaderId: rf.me,
					}, AppendEntriesReply{}
					if rf.sendAppendEntries(peerIdx, &args, &reply) {
						if reply.Term > atomic.LoadInt64(&latestTerm) {
							atomic.StoreInt64(&latestTerm, reply.Term)
						}
					}
					return nil
				})
			}
		}
		eg.Wait()

		if curTerm < latestTerm {
			rf.mu.Lock()
			rf.currentLeader = -1
			rf.currentTerm = latestTerm
			rf.currentRole = Follower
			rf.mu.Unlock()
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		curRole := rf.currentRole
		curTime := rf.latest
		rf.mu.Unlock()
		if curRole == Follower &&
			time.Since(curTime) > timeDurMs(350, 450) {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(timeDurMs(50, 350))
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentRole = Follower
	rf.currentLeader = -1
	rf.currentTerm = 0

	rf.votedFor = make(map[int64]int, 8)
	rf.log = make([]LogEntry, 0, 8)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.latest = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
