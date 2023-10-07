package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	ExecuteTimeout        int64 = 1   // 1s
	ConfigQueryInterval   int64 = 100 // 100ms
	ReConfiguringInterval int64 = 100
)

type ShardState int

const (
	Serving ShardState = iota
	Pulling
	Pushing
	Invalid
)

type shardDB struct {
	state ShardState
	db    map[string]string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Command  string
	ClientId int64
	SeqId    int

	Shards    [shardctrler.NShards]int
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32 // set by Kill()
	lastApplied int
	shardDBs    []shardDB
	configClerk *shardctrler.Clerk
	configNum   int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// if server isn't leader, reject it
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Server%d isn't leader! Reply errWrongLeader.", kv.rf.GetMe())
	}
	// if server isn't responsible for shard, reject it

	// try to log the request

	// to determine whether it is the leader agin

	// creqte the notify chan

	// delete the notify chan
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.lastApplied = 0
	kv.configNum = 0
	kv.shardDBs = make([]shardDB, shardctrler.NShards)

	// Use something like this to talk to the shardctrler:
	kv.configClerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.fetchConfigLoop()
	//go kv.migrateShardLoop()

	return kv
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			DPrintf("Server%d try to apply Log%d, Log: %v.", kv.rf.GetMe(), msg.CommandIndex, msg.Command.(Op))
			// if log already apply, discard it
			if msg.CommandIndex <= kv.lastApplied {
				DPrintf("Server%d has already apply the Log%d, discard it.", kv.rf.GetMe(), msg.CommandIndex)
				kv.mu.Unlock()
				continue
			}

			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			switch op.Command {
			case opConfig:
				// if it't config update log, apply it
				kv.applyConfig(op.ConfigNum, op.Shards)
			default:

			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) fetchConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()

		if kv.isLeader() {
			configNum := kv.configNum
			kv.mu.Unlock()

			newConfig := kv.configClerk.Query(configNum + 1)
			// if config is new, update config
			if newConfig.Num > configNum {
				DPrintf("Server%d update config%d -> %d, newShard: %v", kv.rf.GetMe(), configNum, newConfig.Num, newConfig.Shards)
				kv.updateConfig(newConfig)
			}
		} else {
			kv.mu.Unlock()
		}

		time.Sleep(time.Duration(ConfigQueryInterval) * time.Millisecond)
	}
}

func (kv *ShardKV) applyConfig(num int, shards [10]int) {
	// wait for the last reconfiguration finish
	for !kv.isReConfiguring() {
		time.Sleep(time.Duration(ReConfiguringInterval) * time.Millisecond)
	}

	// update config version
	kv.configNum = num
	// update shard config
	for i, gid := range shards {
		if gid == kv.gid {
			// wait for receving shard
			if kv.shardDBs[i].state == Serving {
				kv.shardDBs[i].state = Pulling
				kv.shardDBs[i].db = make(map[string]string)
			}
		} else {
			// set pushing, and the send shard to new owner
			if kv.shardDBs[i].state == Serving {
				kv.shardDBs[i].state = Pushing
			}
		}
	}
}

func (kv *ShardKV) updateConfig(newConfig shardctrler.Config) {
	kv.mu.Lock()
	// log the new config
	kv.rf.Start(Op{
		Command:   opConfig,
		ConfigNum: newConfig.Num,
		Shards:    newConfig.Shards,
	})
	kv.mu.Unlock()
}

func (kv *ShardKV) isReConfiguring() bool {
	// if any shard is at pulling state or pushing state, that mean reconfiguration doesn't finish
	for _, shardDB := range kv.shardDBs {
		if shardDB.state == Pulling || shardDB.state == Pushing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}
