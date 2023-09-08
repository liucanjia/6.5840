package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverState int

const (
	Follower serverState = iota
	Candidate
	Leader
)

const randFactor float64 = 0.6
const TickInterval int64 = 10 // Loop interval for ticker
const HeartbeatInterval int64 = 100
const BaseHeartbeatTimeout int64 = 200
const BaseElectionTimeout int64 = 1000

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       serverState // server state (follower, candidate or leader)
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int         // candidateId that received vote in current term (-1 if none)

	heartbeatTime time.Time // Leader: broadcast heartbeats interval, Foller: hearbeat timeout that start a new leader election
	electionTime  time.Time // leader election timeout
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dLog2, "S%d recevied appendEntries from S%d at Term T%d.", rf.me, args.LeaderId, rf.currentTerm)

	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is T%d, higher than S%d Term T%d, rejecting append request.", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		Debug(dLog2, "S%d received higher Term T%d appendEntries from S%d, converting to follower.", rf.me, args.Term, args.LeaderId)
		rf.swicthToFollower(args.Term, -1)
	} else if args.Term == rf.currentTerm {
		if rf.state == Candidate {
			Debug(dLog2, "S%d is Candidate, received heartbeat appendEntries from S%d at Term T%d, converting to follower.", rf.me, args.LeaderId, args.Term)
			rf.swicthToFollower(args.Term, -1)
		} else if rf.state == Follower {
			Debug(dLog2, "S%d received heartbeat from S%d at Term T%d.", rf.me, args.LeaderId, args.Term)
			rf.setHeartbeatTimeout(randTimeout(BaseHeartbeatTimeout))
		} else {
			Debug(dError, "S%d is Leader, recevied heartbeat appendEntries from S%d at Term T%d.", rf.me, args.LeaderId, args.Term)
		}
	}

	Debug(dLog2, "S%d reply success to S%d at Term T%d.", rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func randTimeout(BaseTimeout int64) time.Duration {
	extraTime := int64(float64(rand.Int63()%BaseTimeout) * randFactor)

	return time.Duration(BaseTimeout+extraTime) * time.Millisecond
}

func (rf *Raft) setHeartbeatTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout)
	rf.heartbeatTime = t
}

func (rf *Raft) setElectionTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout)
	rf.electionTime = t
}

func (rf *Raft) swicthToFollower(term int, votedFor int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.setHeartbeatTimeout(randTimeout(BaseHeartbeatTimeout))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)

	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d received vote request from S%d at T%d.", rf.me, args.CandidateId, args.Term)
	// if candidate's term < currentTerm, reject vote for candidate
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Term is T%d, higher than candidate S%d Term T%d, rejecting the vote request.", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		return
	} else if args.Term == rf.currentTerm {
		// if not vote for other, vote for candidate
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			Debug(dVote, "S%d granting vote to S%d at Term T%d.", rf.me, args.CandidateId, args.Term)
			rf.swicthToFollower(args.Term, args.CandidateId)

			reply.Term = args.Term
			reply.VotedGranted = true
		} else {
			Debug(dVote, "S%d has vote to S%d, rejecting vote to S%d at Term T%d.", rf.me, rf.votedFor, args.CandidateId, args.Term)
			reply.Term = args.Term
			reply.VotedGranted = false
		}
	} else {
		// vote for candidate
		Debug(dVote, "S%d granting vote to S%d at Term T%d.", rf.me, args.CandidateId, args.Term)
		rf.swicthToFollower(args.Term, args.CandidateId)

		reply.Term = args.Term
		reply.VotedGranted = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (2B).

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

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader && time.Now().After(rf.heartbeatTime) {
			Debug(dTimer, "%S%d, Broadcast heartbeats.", rf.me)
			rf.sendEntries()
		} else if (rf.state == Follower && time.Now().After(rf.heartbeatTime)) || (rf.state == Candidate && time.Now().After(rf.electionTime)) {
			Debug(dTimer, "%S%d, start a new election.", rf.me)
			rf.beginElection()
		}

		rf.mu.Unlock()
		// pause 30 milliseconds.
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.swicthToFollower(0, -1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) beginElection() {
	rf.state = Candidate

	rf.currentTerm++
	Debug(dTerm, "S%d starting a new term. Now at Term T%d.", rf.currentTerm)

	rf.votedFor = rf.me
	rf.setElectionTimeout(randTimeout(BaseElectionTimeout))
	Debug(dTimer, "S%d resetting the electionTimeout, because starting a new election.", rf.me)

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	voteCnt := 1
	var once sync.Once

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		Debug(dVote, "S%d sending vote request to S%d at Term T%d.", rf.me, peer, rf.currentTerm)
		go rf.candidateRequestVote(&voteCnt, args, &once, peer)
	}
}

func (rf *Raft) candidateRequestVote(voteCnt *int, agrs *RequestVoteArgs, once *sync.Once, server int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, agrs, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		Debug(dVote, "S%d receiving vote reply at Term T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dVote, "S%d at Term T%d, receiving lower Term T%d reply from S%d.", rf.me, rf.currentTerm, reply.Term, server)
			return
		} else if reply.Term > rf.currentTerm && !reply.VotedGranted && rf.state == Candidate {
			Debug(dTerm, "S%d at Term T%d, receiving higher Term T%d reply from S%d. Switching to follower State.", rf.me, rf.currentTerm, reply.Term, server)
			once.Do(func() {
				rf.swicthToFollower(reply.Term, -1)
			})
		} else if reply.VotedGranted {
			Debug(dVote, "S%d getting votedGranted for S%d at Term T%d.", rf.me, server, rf.currentTerm)
			*voteCnt++

			if *voteCnt > len(rf.peers)/2 {
				Debug(dLeader, "S%d received majority votedGranted at Term T%d. Becoming a new Leader.", rf.me, rf.currentTerm)
				once.Do(func() {
					rf.state = Leader
					rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval))
					rf.sendEntries()
				})
			}
		} else {
			Debug(dError, "S%d get a error reply at Term T%d from S%d. Reply.Term is %d, reply.VotedGranted is %t.", rf.me, rf.currentTerm, reply.Term, reply.VotedGranted)
		}
	}
}

func (rf *Raft) sendEntries() {
	Debug(dTimer, "S%d resetting heartbeatTimeout, broadcast the heartbeat at Term T%d.", rf.me, rf.currentTerm)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval))

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		go rf.leaderSendEntries(args, peer)
	}
}

func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		Debug(dLog, "S%d received appendEntry reply from S%d at Term T%d.", rf.me, server, rf.currentTerm)

		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term is lower than S%d")
			return
		} else if reply.Term > rf.currentTerm {
			Debug(dLog, "S%d received a higher Term appendEntry reply from S%d at Term T%d, switching to follower.", rf.me, server, reply.Term)
			rf.swicthToFollower(reply.Term, -1)
		}
	}
}
