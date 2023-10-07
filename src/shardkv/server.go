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
	MigrateInterval       int64 = 100
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

	Config shardctrler.Config
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
	config      shardctrler.Config
	scClerk     *shardctrler.Clerk
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

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {

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
	kv.config = shardctrler.Config{}
	kv.shardDBs = make([]shardDB, shardctrler.NShards)

	// Use something like this to talk to the shardctrler:
	kv.scClerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.fetchConfigLoop()
	go kv.migrateShardLoop()

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
				kv.applyConfig(op.Config)
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
			configNum := kv.config.Num
			kv.mu.Unlock()

			newConfig := kv.scClerk.Query(configNum + 1)
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

func (kv *ShardKV) migrateShardLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		for idx, shardDB := range kv.shardDBs {
			if shardDB.state == Pushing {
				// try to send shard to new owner
				args := SendShardArgs{
					configNum: kv.config.Num,
					shard:     make(map[string]string),
				}
				for key, value := range shardDB.db {
					args.shard[key] = value
				}

				gid := kv.config.Shards[idx]
				DPrintf("Server%d send shard%d to Group%d in config%d.", kv.rf.GetMe(), idx, gid, kv.config.Num)
				if servers, ok := kv.config.Groups[gid]; ok {
					// try each server for the shard.
					for si := 0; si < len(servers); si = (si + 1) % len(servers) {
						srv := kv.make_end(servers[si])
						var reply SendShardReply
						ok := srv.Call("ShardKV.SendShard", &args, &reply)
						if ok && reply.Err == OK {
							break
						}
					}
				}
			}
		}

		kv.mu.Unlock()
		time.Sleep(time.Duration(MigrateInterval) * time.Millisecond)
	}
}

func (kv *ShardKV) applyConfig(newConfig shardctrler.Config) {
	// wait for the last reconfiguration finish
	for !kv.isReConfiguring() {
		time.Sleep(time.Duration(ReConfiguringInterval) * time.Millisecond)
	}

	// update config
	kv.config.Num = newConfig.Num
	kv.config.Shards = newConfig.Shards
	kv.config.Groups = make(map[int][]string)
	for gid, servers := range newConfig.Groups {
		kv.config.Groups[gid] = append([]string{}, servers...)
	}
	// update shard state
	for i, gid := range newConfig.Shards {
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
	command := Op{
		Command: opConfig,
		Config: shardctrler.Config{
			Num:    newConfig.Num,
			Shards: newConfig.Shards,
			Groups: make(map[int][]string),
		},
	}
	for gid, servers := range newConfig.Groups {
		command.Config.Groups[gid] = append([]string{}, servers...)
	}
	kv.rf.Start(command)
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
