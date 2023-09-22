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

	"bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogEntries []LogEntry

func (rf *Raft) getEntry(index int) *LogEntry {
	logIndex := index - rf.lastIncludedIndex

	if logIndex < 0 {
		Debug(dError, "S%d LogIndex is invaild, index is %d, lower than lastIncludedIndex %d.", rf.me, index, rf.lastIncludedIndex)
		os.Exit(-1)
	} else if logIndex == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	} else if logIndex > len(rf.logs) {
		return &LogEntry{
			Command: nil,
			Term:    0,
		}
	}

	return &rf.logs[logIndex-1]
}

func (rf *Raft) getLastInfo() (int, int) {
	lastLogIndex := len(rf.logs) + rf.lastIncludedIndex
	lastLogTerm := rf.getEntry(lastLogIndex).Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getLogsSlice(begin, end int) LogEntries {
	logsBegin := begin - rf.lastIncludedIndex
	logsEnd := end - rf.lastIncludedIndex
	if logsBegin <= 0 || logsEnd > len(rf.logs)+1 || end < begin {
		Debug(dError, "LogsSlice range is invaild, begin is %d, end is %d, lastIncludedIndex is %d, logs len is %d.", begin, end, rf.lastIncludedIndex, rf.lastIncludedIndex+len(rf.logs))
		os.Exit(-1)
	}

	newLogs := LogEntries{}
	newLogs = append(newLogs, rf.logs[logsBegin-1:logsEnd-1]...)
	return newLogs
}

const randFactor float64 = 0.6
const TickInterval int64 = 10 // Loop interval for ticker
const HeartbeatInterval int64 = 100
const BaseHeartbeatTimeout int64 = 300
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
	state         serverState // server state (follower, candidate or leader)
	heartbeatTime time.Time   // Leader: broadcast heartbeats interval, Foller: hearbeat timeout that start a new leader election
	electionTime  time.Time   // leader election timeout

	// Persistent state on all servers:
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (-1 if none)
	logs        LogEntries // log entries

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// The last Entry info in the Snapshot
	snapshot          []byte // tmp snapshot in memory
	lastIncludedTerm  int    // 0 if Snapshot is empty
	lastIncludedIndex int    // 0 if Snapshot is empty
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      LogEntries
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int //term in the conflicting entry (if any), 0 mean entry is empty
	XIndex  int //index of first entry with that term (if any)
	XLen    int //log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is T%d, higher than S%d Term T%d, rejecting append request.", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		Debug(dLog2, "S%d received higher Term T%d appendEntries from S%d, converting to follower.", rf.me, args.Term, args.LeaderId)
		rf.swicthToFollower(args.Term, -1, true)
	} else if args.Term == rf.currentTerm {
		if rf.state == Candidate {
			Debug(dLog2, "S%d is Candidate, received heartbeat appendEntries from S%d at Term T%d, converting to follower.", rf.me, args.LeaderId, args.Term)
			rf.swicthToFollower(args.Term, -1, true)
		} else if rf.state == Follower {
			Debug(dLog2, "S%d received heartbeat from S%d at Term T%d.", rf.me, args.LeaderId, args.Term)
			rf.setHeartbeatTimeout(randTimeout(BaseHeartbeatTimeout))
		} else {
			Debug(dError, "S%d is Leader, recevied heartbeat appendEntries from S%d at Term T%d.", rf.me, args.LeaderId, args.Term)
		}
	}

	rf.matchLog(args, reply)
}

func randTimeout(BaseTimeout int64) time.Duration {
	extraTime := int64(float64(rand.Int63()%BaseTimeout) * randFactor)

	return time.Duration(BaseTimeout + extraTime)
}

func (rf *Raft) setHeartbeatTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout * time.Millisecond)
	rf.heartbeatTime = t
}

func (rf *Raft) setElectionTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) swicthToFollower(term int, votedFor int, refreshTimeout bool) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.persist(nil)
	if refreshTimeout {
		rf.setHeartbeatTimeout(randTimeout(BaseHeartbeatTimeout))
	}
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

func (rf *Raft) GetMe() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	Debug(dPersist, "S%d saving previously persisted state to persister.", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(rf.currentTerm)
	if err != nil {
		Debug(dError, "S%d failed to decode currentTerm.", rf.me)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		Debug(dError, "S%d failed to decode votedFor.", rf.me)
	}
	err = e.Encode(rf.logs)
	if err != nil {
		Debug(dError, "S%d failed to decode logs.", rf.me)
	}

	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		Debug(dError, "S%d failed to decode lastIncludedIndex.", rf.me)
	}

	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		Debug(dError, "S%d failed to decode lastIncludedTerm.", rf.me)
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
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
	Debug(dPersist, "S%d restoring previously persisted state from persister.", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err := d.Decode(&rf.currentTerm)
	if err != nil {
		Debug(dError, "S%d can not read currentTerm from persister.", rf.me)
	}

	err = d.Decode(&rf.votedFor)
	if err != nil {
		Debug(dError, "S%d can not read votedFor from persister.", rf.me)
	}

	err = d.Decode(&rf.logs)
	if err != nil {
		Debug(dError, "S%d can not read logs from persister.", rf.me)
	}

	err = d.Decode(&rf.lastIncludedIndex)
	if err != nil {
		Debug(dError, "S%d can not read lastIncludedIndex from persister.", rf.me)
	}

	err = d.Decode(&rf.lastIncludedTerm)
	if err != nil {
		Debug(dError, "S%d can not read lastIncludedTerm from persister.", rf.me)
	}
}

type InstallSnapshotArgs struct {
	Term             int    // leader’s term
	LeaderId         int    // leader's id
	LastIncludeIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int    // term of lastIncludedIndex
	Snapshot         []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "S%d received install snapshot request from S%d at Term T%d.", rf.me, args.LeaderId, rf.currentTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d rejects to insatll snapshot because Term T%d higher than S%d Term T%d.", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	} else if args.LastIncludeIndex <= rf.lastIncludedIndex {
		Debug(dSnap, "S%d rejects to insatll snapshot because lastIncludedIndex %d do not smaller than S%d lastIncludedIndex %d.", rf.me, rf.currentTerm, rf.lastIncludedIndex, args.LastIncludeIndex)
		return
	}

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == Candidate) {
		rf.swicthToFollower(args.Term, -1, true)
	}

	rf.snapshot = append([]byte{}, args.Snapshot...)
	// cut the old logEntry
	lastLogIndex, _ := rf.getLastInfo()
	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if lastLogIndex > rf.lastIncludedIndex {
		rf.logs = rf.getLogsSlice(rf.lastIncludedIndex+1, lastLogIndex+1)
	} else {
		rf.logs = LogEntries{}
	}

	rf.persist(rf.snapshot)
	reply.Term = rf.currentTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	Debug(dSnap, "S%d try to snapshotting through index %d.", rf.me, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastApplied {
		Debug(dSnap, "S%d can snapshot because invaild index %d, lastIncludedIndex is %d, lastApplied is %d.", rf.me, index, rf.lastIncludedIndex, rf.lastApplied)
		return
	}

	lastLogIndex, _ := rf.getLastInfo()
	rf.lastIncludedTerm = rf.getEntry(index).Term
	rf.logs = rf.getLogsSlice(index+1, lastLogIndex+1)
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot

	rf.persist(rf.snapshot)
}

func (rf *Raft) sendSnapshot(server int) {
	Debug(dSnap, "S%d send snapshot to S%d at Term T%d.", rf.me, server, rf.currentTerm)

	args := &InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Snapshot:         rf.snapshot,
	}

	go rf.leaderSendSnapshot(args, server)
}

func (rf *Raft) leaderSendSnapshot(args *InstallSnapshotArgs, server int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		Debug(dSnap, "S%d received installSnapshot reply from S%d at Term T%d.", rf.me, server, rf.currentTerm)

		if rf.currentTerm < reply.Term {
			Debug(dSnap, "S%d Term is T%d, lower than S%d Term T%d, switching to follower.", rf.me, rf.currentTerm, server, reply.Term)
			rf.swicthToFollower(reply.Term, -1, true)
			return
		} else if rf.currentTerm > reply.Term {
			Debug(dSnap, "S%d Term is T%d, higher than S%d Term T%d, invaild reply.", rf.me, rf.currentTerm, server, reply.Term)
			return
		}

		rf.matchIndex[server] = max(args.LastIncludeIndex, rf.matchIndex[server])
		rf.nextIndex[server] = max(args.LastIncludeIndex+1, rf.nextIndex[server])
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	}

	if args.Term > rf.currentTerm {
		Debug(dVote, "S%d Term is T%d, lower than Term T%d, switching to follower and update the Term.", rf.me, rf.currentTerm, args.Term)
		rf.swicthToFollower(args.Term, -1, false)
	}

	reply.Term = rf.currentTerm

	flag := false
	lastLogIndex, lastLogTerm := rf.getLastInfo()
	if args.LastLogTerm > lastLogTerm {
		flag = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		flag = true
	}

	if !flag {
		reply.VotedGranted = false
		return
	}

	// if not vote for other, vote for candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		Debug(dVote, "S%d granting vote to S%d at Term T%d.", rf.me, args.CandidateId, args.Term)
		rf.swicthToFollower(args.Term, args.CandidateId, true)
		reply.VotedGranted = true
	} else {
		Debug(dVote, "S%d has vote to S%d, rejecting vote to S%d at Term T%d.", rf.me, rf.votedFor, args.CandidateId, args.Term)
		reply.VotedGranted = false
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		Debug(dLog, "S%d is not leader.", rf.me)
		isLeader = false
		return index, term, isLeader
	}

	log := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, log)
	rf.persist(nil)
	index, term = rf.getLastInfo()
	Debug(dLog, "S%d Add command at Term T%d, Command: %v.", rf.me, rf.currentTerm, command)
	rf.sendEntries(false)

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
			Debug(dTimer, "S%d, Broadcast heartbeats.", rf.me)
			rf.sendEntries(true)
		} else if (rf.state == Follower && time.Now().After(rf.heartbeatTime)) || (rf.state == Candidate && time.Now().After(rf.electionTime)) {
			Debug(dTimer, "S%d, start a new election.", rf.me)
			rf.beginElection()
		}

		rf.mu.Unlock()
		// pause 10 milliseconds.
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
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.setHeartbeatTimeout(randTimeout(BaseHeartbeatTimeout))
	// start ticker goroutine to start elections
	go rf.ticker()

	// apply LogEntries to state machine
	go rf.applyLogsLoop(applyCh)

	return rf
}

func (rf *Raft) beginElection() {
	rf.state = Candidate

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(nil)
	Debug(dTerm, "S%d starting a new term. Now at Term T%d.", rf.me, rf.currentTerm)

	rf.setElectionTimeout(randTimeout(BaseElectionTimeout))
	Debug(dTimer, "S%d resetting the electionTimeout, because starting a new election.", rf.me)

	lastLogIndex, lastLogTerm := rf.getLastInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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

		Debug(dVote, "S%d receiving vote reply for S%d at Term T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dVote, "S%d at Term T%d, receiving lower Term T%d reply from S%d.", rf.me, rf.currentTerm, reply.Term, server)
			return
		} else if reply.Term > rf.currentTerm && !reply.VotedGranted && rf.state == Candidate {
			Debug(dTerm, "S%d at Term T%d, receiving higher Term T%d reply from S%d. Switching to follower State.", rf.me, rf.currentTerm, reply.Term, server)
			once.Do(func() {
				rf.swicthToFollower(reply.Term, -1, true)
			})
		} else if reply.VotedGranted {
			Debug(dVote, "S%d getting votedGranted for S%d at Term T%d.", rf.me, server, rf.currentTerm)
			*voteCnt++

			if *voteCnt > len(rf.peers)/2 {
				once.Do(func() {
					Debug(dLeader, "S%d received majority votedGranted at Term T%d. Becoming a new Leader.", rf.me, rf.currentTerm)
					rf.state = Leader
					rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval))

					lastLogIndex, _ := rf.getLastInfo()
					for peer := range rf.peers {
						rf.nextIndex[peer] = lastLogIndex + 1
						rf.matchIndex[peer] = 0
					}
					rf.sendEntries(true)
				})
			}
		} else {
			Debug(dError, "S%d get a error reply at Term T%d from S%d. Reply.Term is %d, reply.VotedGranted is %t.", rf.me, rf.currentTerm, server, reply.Term, reply.VotedGranted)
		}
	}
}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, lastLogIndex, server int) {
	nextIndex := rf.nextIndex[server]

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.getEntry(args.PrevLogIndex).Term
	args.LeaderCommit = rf.commitIndex

	if lastLogIndex >= nextIndex {
		Debug(dLog, "S%d send logEnrties from %d -> %d to S%d.", rf.me, nextIndex, lastLogIndex, server)
		args.Entries = make(LogEntries, lastLogIndex-nextIndex+1)
		copy(args.Entries, rf.getLogsSlice(nextIndex, lastLogIndex+1))
	}
}

func (rf *Raft) sendEntries(isHeartBeat bool) {
	if isHeartBeat {
		Debug(dTimer, "S%d resetting heartbeatTimeout, broadcast the heartbeat at Term T%d.", rf.me, rf.currentTerm)
		rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval))
	}

	lastLogIndex, _ := rf.getLastInfo()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else if rf.nextIndex[peer] <= rf.lastIncludedIndex {
			// follower backward too much
			rf.sendSnapshot(peer)
			continue
		}

		args := &AppendEntriesArgs{}
		rf.buildAppendEntriesArgs(args, lastLogIndex, peer)

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
			Debug(dLog, "S%d Term is lower than S%d", server, rf.me)
			return
		} else if reply.Term > rf.currentTerm {
			Debug(dLog, "S%d received a higher Term appendEntry reply from S%d at Term T%d, switching to follower.", rf.me, server, reply.Term)
			rf.swicthToFollower(reply.Term, -1, true)
		} else {
			if args.Term < rf.currentTerm {
				Debug(dLog, "S%d Term T%d has changed, the appendEntry request Term T%d is outdated.", rf.me, rf.currentTerm, args.Term)
				return
			}

			if reply.Success {
				Debug(dLog, "S%d and S%d logEntries in sync at Term T%d.", rf.me, server, rf.currentTerm)
				newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = max(newNextIndex, rf.nextIndex[server])
				rf.matchIndex[server] = max(newMatchIndex, rf.matchIndex[server])
				rf.updateCommitIndex()
			} else {
				Debug(dLog, "S%d logEntries are not matching with S%d, decrement nextIndex and retry.", rf.me, server)

				rf.updateNextIndex(reply.XTerm, reply.XIndex, reply.XLen, server)
				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					rf.sendSnapshot(server)
				} else {
					lastLogIndex, _ := rf.getLastInfo()
					newArgs := &AppendEntriesArgs{}
					rf.buildAppendEntriesArgs(newArgs, lastLogIndex, server)
					go rf.leaderSendEntries(newArgs, server)
				}
			}
		}
	}
}

func (rf *Raft) updateNextIndex(xTerm, xIndex, xLen, server int) {
	if xTerm == 0 {
		rf.nextIndex[server] = xLen + 1
		return
	}

	flag := false
	idx := rf.nextIndex[server]
	// nextIndex at least must be 1
	for ; idx > 1 && idx > rf.matchIndex[server]; idx-- {
		if rf.getEntry(idx).Term == xTerm {
			flag = true
			break
		}
	}

	if !flag {
		rf.nextIndex[server] = xIndex
	} else {
		rf.nextIndex[server] = idx
	}

}

func (rf *Raft) updateCommitIndex() {
	N, _ := rf.getLastInfo()
	for ; N > rf.commitIndex && rf.getEntry(N).Term == rf.currentTerm; N-- {
		cnt := 1
		for peer, matchIndex := range rf.matchIndex {
			if peer == rf.me {
				continue
			} else if matchIndex >= N {
				cnt++
			}
		}

		if cnt > len(rf.peers)/2 {
			// only log entries from the leader's currrent term are allowed to be committed
			if rf.getEntry(N).Term == rf.currentTerm {
				rf.commitIndex = N
				Debug(dCommit, "S%d update CommitIndex to %d at Term T%d.", rf.me, rf.commitIndex, rf.currentTerm)
			}
			break
		}
	}
}

func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()

		applyMsgs := []ApplyMsg{}

		// snapshot advance the service's state, install snapshot to service
		if rf.snapshot != nil && rf.lastIncludedIndex > rf.lastApplied {
			rf.lastApplied = rf.lastIncludedIndex
			rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)

			msg := ApplyMsg{
				SnapshotValid: true,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}

			msg.Snapshot = append([]byte{}, rf.snapshot...)
			applyMsgs = append(applyMsgs, msg)
		} else {
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++

				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.getEntry(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				}
				applyMsgs = append(applyMsgs, msg)

				Debug(dLog, "S%d apply log %d at Term T%d.", rf.me, rf.lastApplied, rf.currentTerm)
			}
		}

		rf.mu.Unlock()

		for _, msg := range applyMsgs {
			applyCh <- msg
		}

		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

func (rf *Raft) matchLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm

	// if logEntries have been snapshot, discard the old logEntry
	if args.PrevLogIndex < rf.lastIncludedIndex {
		newLogBeginIndex := rf.lastIncludedIndex - args.PrevLogIndex
		newArgs := &AppendEntriesArgs{
			Term:         args.Term,
			LeaderId:     args.LeaderId,
			PrevLogIndex: rf.lastIncludedIndex,
			PrevLogTerm:  rf.lastIncludedTerm,
			Entries:      args.Entries[newLogBeginIndex:],
			LeaderCommit: args.LeaderCommit,
		}
		args = newArgs
		Debug(dLog2, "S%d snapshot update than logEntries send from S%d, discard the old logEntry.", rf.me, args.LeaderId)
	}

	entry := rf.getEntry(args.PrevLogIndex)
	if entry.Term != args.PrevLogTerm {
		Debug(dLog2, "S%d prev logEntry do not match at index %d.", rf.me, args.PrevLogIndex)
		reply.Success = false
		reply.XTerm = entry.Term
		if reply.XTerm == 0 {
			// log too short, entry is nil
			reply.XLen, _ = rf.getLastInfo()
		} else {
			idx := args.PrevLogIndex
			for idx >= rf.lastIncludedIndex && rf.getEntry(idx).Term == reply.XTerm {
				idx--
			}
			reply.XIndex = idx + 1
		}
	} else {
		for i, leaderEntry := range args.Entries {
			followerEntryIndex := args.PrevLogIndex + i + 1
			entry = rf.getEntry(followerEntryIndex)
			if leaderEntry.Term != entry.Term {
				rf.logs = append(rf.getLogsSlice(rf.lastIncludedIndex+1, followerEntryIndex), args.Entries[i:]...)
				rf.persist(nil)
				Debug(dLog2, "S%d append logEntries success, now logs len is %d.", rf.me, len(rf.logs)+rf.lastIncludedIndex)
				break
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			Debug(dLog2, "S%d update CommitIndex to %d at Term T%d as a follower.", rf.me, rf.commitIndex, rf.currentTerm)
		}
		reply.Success = true
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
