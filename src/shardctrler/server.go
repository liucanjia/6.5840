package shardctrler

import (
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ExecuteTimeout int64 = 1

type ReplyMsg struct {
	SeqId       int
	WrongLeader bool
	Err         Err
	Config      Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int
	configs     []Config // indexed by config num
	notifyChans map[int]chan ReplyMsg
	lastOpReply map[int64]ReplyMsg
}

type Op struct {
	// Your data here.
	Client  ClientInfo
	Command string
	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	// if server isn't leader, reply err
	if reply.WrongLeader = !sc.isLeader(); reply.WrongLeader {
		DPrintf("SC%d isn't leader! Reply WrongLeader.", sc.me)
		sc.mu.Unlock()
		return
	}

	// if request is duplicate, reply the last result
	if sc.isDuplicateRequest(args.Client) {
		reply.Err = sc.lastOpReply[args.Client.ClientId].Err
		DPrintf("SC%d received duplicate request. Reply the last result: %v.", sc.me, reply)
		return
	}

	// first log the request
	LogEntry := Op{
		Client:  args.Client,
		Command: opJoin,
		Servers: make(map[int][]string),
	}
	for k, v := range args.Servers {
		LogEntry.Servers[k] = v
	}

	index, _, isLeader := sc.rf.Start(LogEntry)
	// to determine whether it is the leader again
	if !isLeader {
		DPrintf("SC%d isn't leader! Reply wrongLeader.", sc.me)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	//create the notify chan
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	// wait for notify
	select {
	case msg := <-ch:
		reply.WrongLeader, reply.Err = msg.WrongLeader, msg.Err
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("SC%d Execute Log%d Timeout! Log%d is %v", sc.me, index, index, LogEntry)
		reply.Err = ErrExecuteTimeout
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	// if server isn't leader, reply err
	if reply.WrongLeader = !sc.isLeader(); reply.WrongLeader {
		DPrintf("SC%d isn't leader! Reply WrongLeader.", sc.me)
		sc.mu.Unlock()
		return
	}

	// if request is duplicate, reply the last result
	if sc.isDuplicateRequest(args.Client) {
		reply.Err = sc.lastOpReply[args.Client.ClientId].Err
		DPrintf("SC%d received duplicate request. Reply the last result: %v.", sc.me, reply)
		return
	}

	// first log the request
	LogEntry := Op{
		Client:  args.Client,
		Command: opLeave,
		GIDs:    make([]int, len(args.GIDs)),
	}
	copy(LogEntry.GIDs, args.GIDs)
	index, _, isLeader := sc.rf.Start(LogEntry)
	// to determine whether it is the leader again
	if !isLeader {
		DPrintf("SC%d isn't leader! Reply wrongLeader.", sc.me)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	//create the notify chan
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	// wait for notify
	select {
	case msg := <-ch:
		reply.WrongLeader, reply.Err = msg.WrongLeader, msg.Err
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("SC%d Execute Log%d Timeout! Log%d is %v", sc.me, index, index, LogEntry)
		reply.Err = ErrExecuteTimeout
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	// if server isn't leader, reply err
	if reply.WrongLeader = !sc.isLeader(); reply.WrongLeader {
		DPrintf("SC%d isn't leader! Reply WrongLeader.", sc.me)
		sc.mu.Unlock()
		return
	}

	// if request is duplicate, reply the last result
	if sc.isDuplicateRequest(args.Client) {
		reply.Err = sc.lastOpReply[args.Client.ClientId].Err
		DPrintf("SC%d received duplicate request. Reply the last result: %v.", sc.me, reply)
		return
	}

	// first log the request
	LogEntry := Op{
		Client:  args.Client,
		Command: opMove,
		Shard:   args.Shard,
		GID:     args.GID,
	}
	index, _, isLeader := sc.rf.Start(LogEntry)
	// to determine whether it is the leader again
	if !isLeader {
		DPrintf("SC%d isn't leader! Reply wrongLeader.", sc.me)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	//create the notify chan
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	// wait for notify
	select {
	case msg := <-ch:
		reply.WrongLeader, reply.Err = msg.WrongLeader, msg.Err
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("SC%d Execute Log%d Timeout! Log%d is %v", sc.me, index, index, LogEntry)
		reply.Err = ErrExecuteTimeout
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	// if server isn't leader, reply err
	if reply.WrongLeader = !sc.isLeader(); reply.WrongLeader {
		DPrintf("SC%d isn't leader! Reply WrongLeader.", sc.me)
		sc.mu.Unlock()
		return
	}

	// if request is duplicate, reply the last result
	if sc.isDuplicateRequest(args.Client) {
		reply.Err = sc.lastOpReply[args.Client.ClientId].Err
		sc.copyConfig(&reply.Config, sc.lastOpReply[args.Client.ClientId].Config)
		DPrintf("SC%d received duplicate request. Reply the last result: %v.", sc.me, reply)
		return
	}

	// first log the request
	LogEntry := Op{
		Client:  args.Client,
		Command: opQuery,
		Num:     args.Num,
	}
	index, _, isLeader := sc.rf.Start(LogEntry)
	// to determine whether it is the leader again
	if !isLeader {
		DPrintf("SC%d isn't leader! Reply wrongLeader.", sc.me)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	//create the notify chan
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	// wait for notify
	select {
	case msg := <-ch:
		reply.WrongLeader, reply.Err = msg.WrongLeader, msg.Err
		sc.copyConfig(&reply.Config, msg.Config)
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("SC%d Execute Log%d Timeout! Log%d is %v", sc.me, index, index, LogEntry)
		reply.Err = ErrExecuteTimeout
	}
}

func (sc *ShardCtrler) createNewConfig() Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{}
	sc.copyConfig(&newConfig, lastConfig)
	newConfig.Num++
	return newConfig
}

func (sc *ShardCtrler) executeJoin(servers map[int][]string) {
	DPrintf("SC%d execute Join! Servers is %v.", sc.me, servers)

	newConfig := sc.createNewConfig()
	for gid, group := range servers {
		newConfig.Groups[gid] = make([]string, len(group))
		copy(newConfig.Groups[gid], group)
	}

	sc.assignShardToGroup(&newConfig)
	sc.configs = append(sc.configs, newConfig)

	sc.showLatestConfig()
}

func (sc *ShardCtrler) executeLeave(gids []int) {
	DPrintf("SC%d execute Leave! Gids is %v.", sc.me, gids)

	newConfig := sc.createNewConfig()
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		for i := 0; i < NShards; i++ {
			if newConfig.Shards[i] == gid {
				newConfig.Shards[i] = 0
			}
		}
	}

	sc.assignShardToGroup(&newConfig)
	sc.configs = append(sc.configs, newConfig)

	sc.showLatestConfig()
}

func (sc *ShardCtrler) executeMove(shard, gid int) {
	DPrintf("SC%d execute Move! Shard is %d, Gid is %d.", sc.me, shard, gid)

	newConfig := sc.createNewConfig()
	newConfig.Shards[shard] = gid

	sc.configs = append(sc.configs, newConfig)

	sc.showLatestConfig()
}

func (sc *ShardCtrler) executeQuery(num int) Config {
	DPrintf("SC%d execute Query! Num is %d.", sc.me, num)

	maxConfigNum := len(sc.configs) - 1
	if num == -1 || num >= maxConfigNum {
		return sc.configs[maxConfigNum]
	} else {
		return sc.configs[num]
	}
}

func (sc *ShardCtrler) getMinGid(config *Config) (int, int) {
	gidCnts := make(map[int]int)

	for gid := range config.Groups {
		gidCnts[gid] = 0
	}

	for _, gid := range config.Shards {
		if gid != 0 {
			gidCnts[gid]++
		}
	}

	minGid, minGidCnt := 0, NShards
	for gid, gidCnt := range gidCnts {
		if gidCnt < minGidCnt {
			minGid, minGidCnt = gid, gidCnt
		} else if gidCnt == minGidCnt && gid < minGid {
			minGid = gid
		}
	}
	return minGid, minGidCnt
}

func (sc *ShardCtrler) getMaxGid(config *Config) (int, int) {
	gidCnts := make(map[int]int)

	for gid := range config.Groups {
		gidCnts[gid] = 0
	}

	for _, gid := range config.Shards {
		if gid != 0 {
			gidCnts[gid]++
		}
	}

	maxGid, maxGidCnt := 0, 0
	for gid, gidCnt := range gidCnts {
		if gidCnt > maxGidCnt {
			maxGid, maxGidCnt = gid, gidCnt
		} else if gidCnt == maxGidCnt && gid < maxGid {
			maxGid = gid
		}
	}
	return maxGid, maxGidCnt
}

func (sc *ShardCtrler) assignShardToGroup(config *Config) {
	// if shard isn't assign, assign it
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == 0 {
			config.Shards[i], _ = sc.getMinGid(config)
		}
	}

	// balance the Shard
	for {
		minGid, minGidCnt := sc.getMinGid(config)
		maxGid, maxGidCnt := sc.getMaxGid(config)

		if maxGidCnt-minGidCnt <= 1 {
			break
		} else {
			for i, tag := 0, true; i < NShards && tag; i++ {
				if config.Shards[i] == maxGid {
					config.Shards[i] = minGid
					tag = false
				}
			}
		}
	}
}

func (sc *ShardCtrler) isLeader() bool {
	_, isLeader := sc.rf.GetState()
	return isLeader
}

func (sc *ShardCtrler) copyConfig(dst *Config, src Config) {
	if dst == nil {
		return
	}

	dst.Num = src.Num
	dst.Shards = src.Shards
	dst.Groups = make(map[int][]string)
	for k, v := range src.Groups {
		dst.Groups[k] = v
	}
}

func (sc *ShardCtrler) showLatestConfig() {
	latestConfig := sc.configs[len(sc.configs)-1]

	DPrintf("SC%d show the latest Config{num: %d, shards: %v, groups: %v}.", sc.me, latestConfig.Num, latestConfig.Shards, latestConfig.Groups)
}

func (sc *ShardCtrler) makeNotifyChan(index int) chan ReplyMsg {
	sc.notifyChans[index] = make(chan ReplyMsg)
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) getNotifyChan(index int) chan ReplyMsg {
	if _, ok := sc.notifyChans[index]; ok {
		return sc.notifyChans[index]
	} else {
		return nil
	}
}

func (sc *ShardCtrler) isDuplicateRequest(client ClientInfo) bool {
	if reply, ok := sc.lastOpReply[client.ClientId]; ok {
		return reply.SeqId == client.SeqId
	} else {
		return false
	}
}

func (sc *ShardCtrler) applyLogEntry(op Op) ReplyMsg {
	replyMsg := ReplyMsg{
		SeqId:       op.Client.SeqId,
		WrongLeader: false,
		Err:         OK,
	}

	switch op.Command {
	case opJoin:
		sc.executeJoin(op.Servers)
	case opLeave:
		sc.executeLeave(op.GIDs)
	case opMove:
		sc.executeMove(op.Shard, op.GID)
	case opQuery:
		replyMsg.Config = sc.executeQuery(op.Num)
	}
	return replyMsg
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			DPrintf("SC%d try to apply Log%d: %v.", sc.me, msg.CommandIndex, msg.Command.(Op))
			// if log already apply, discard it
			if msg.CommandIndex <= sc.lastApplied {
				DPrintf("SC%d discard Log%d that already apply.", sc.me, msg.CommandIndex)
			} else {
				var replyMsg ReplyMsg
				sc.lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				// if duplicate request , don't apply log
				if sc.isDuplicateRequest(op.Client) {
					DPrintf("SC%d doesn't apply Log%d, because it's duplicate.", sc.me, msg.CommandIndex)
				} else {
					sc.lastOpReply[op.Client.ClientId] = sc.applyLogEntry(op)
				}
				lastReply := sc.lastOpReply[op.Client.ClientId]
				replyMsg.WrongLeader, replyMsg.Err = lastReply.WrongLeader, lastReply.Err
				if op.Command == opQuery {
					sc.copyConfig(&replyMsg.Config, lastReply.Config)
				}

				// if server is leader, reply to client
				if _, isLeader := sc.rf.GetState(); isLeader {
					if ch := sc.getNotifyChan(msg.CommandIndex); ch != nil {
						ch <- replyMsg
					}
				}
			}
			sc.mu.Unlock()
		} else {
			log.Fatalf("SC%d: invaild Log%d: %v.", sc.me, msg.CommandIndex, msg.Command)
		}

	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.notifyChans = make(map[int]chan ReplyMsg)
	sc.lastOpReply = make(map[int64]ReplyMsg)

	go sc.applier()

	return sc
}
