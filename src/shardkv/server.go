package shardkv

import (
	"bytes"
	"log"
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

	InitConfigNum int = 1
)

type ShardState int

const (
	Invalid ShardState = iota
	Serving
	Pulling
	Pushing
)

type ShardDB struct {
	State ShardState
	DB    map[string]string
}

type ShardInfo struct {
	Num        int
	Idx        int
	Shard      map[string]string
	Client2Seq map[int64]int
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
	Shard  ShardInfo
}

type ApplyRes struct {
	Err      Err
	Value    string
	ClientId int64
	SeqId    int
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
	shardDBs    []ShardDB
	config      shardctrler.Config
	scClerk     *shardctrler.Clerk
	notifyChans map[int]chan ApplyRes
	client2Seq  map[int64]int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// if server isn't leader, reject it
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Group%dServer%d isn't leader! Reply errWrongLeader.", kv.gid, kv.me)
		return
	}
	// if server isn't responsible for shard, reject it
	if idx := key2shard(args.Key); kv.config.Shards[idx] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("Group%dServer%d isn't responsible for key%v.", kv.gid, kv.me, args.Key)
		return
	}
	// if shard isn't at serving, return pause serve
	if idx := key2shard(args.Key); kv.shardDBs[idx].State != Serving {
		reply.Err = ErrPauseServe
		kv.mu.Unlock()
		DPrintf("Group%dServer%d pause serving.", kv.gid, kv.me)
		return
	}
	// if request is duplicate, reply the last result
	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		reply.Value, reply.Err = kv.get(args.Key)
		kv.mu.Unlock()
		DPrintf("DuplicateRequest! Reply is %v.", reply)
		return
	}
	// try to log the request
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Command:  opGet,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})
	DPrintf("Group%dServer%d add Log%d{ClientId:%d, SeqId:%d, opGet, key:%v}", kv.gid, kv.me, index, args.ClientId, args.SeqId, args.Key)
	// to determine whether it is the leader agin
	if !isLeader {
		DPrintf("Group%dServer%d isn't leader! Reply errWrongLeader.", kv.gid, kv.me)
		kv.mu.Unlock()
		return
	}
	// create the notify chan
	ch := kv.makeNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("Group%dServer%d execute Timeout!", kv.gid, kv.me)
		reply.Err = ErrExecuteTimeout
	}
	// delete the notify chan
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Group%dServer%d receive %v request from Client%d, key is %v, value is %v.", kv.gid, kv.me, args.Op, args.ClientId, args.Key, args.Value)
	kv.mu.Lock()
	// if server isn't leader, reject it
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Group%dServer%d isn't leader! Reply errWrongLeader.", kv.gid, kv.me)
		return
	}
	// if server isn't responsible for shard, reject it
	if idx := key2shard(args.Key); kv.config.Shards[idx] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("Group%dServer%d isn't responsible for key%v.", kv.gid, kv.me, args.Key)
		return
	}
	// if shard isn't at serving, return pause serve
	if idx := key2shard(args.Key); kv.shardDBs[idx].State != Serving {
		reply.Err = ErrPauseServe
		kv.mu.Unlock()
		DPrintf("Group%dServer%d pause serving.", kv.gid, kv.me)
		return
	}
	// if request is duplicate, reply the last result
	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		reply.Err = OK
		kv.mu.Unlock()
		DPrintf("DuplicateRequest! Rply is %v.", reply)
		return
	}
	// try to log the request
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		Command:  args.Op,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})
	DPrintf("Group%dServer%d add Log%d:{ClientId:%d, SeqId:%d, %v, key:%v, value:%v}", kv.gid, kv.me, index, args.ClientId, args.SeqId, args.Op, args.Key, args.Value)
	// to determine whether it is the leader agin
	if !isLeader {
		DPrintf("Group%dServer%d isn't leader! Reply errWrongLeader.", kv.gid, kv.me)
		kv.mu.Unlock()
		return
	}
	// create the notify chan
	ch := kv.makeNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("Group%dServer%d execute Timeout!", kv.gid, kv.me)
		reply.Err = ErrExecuteTimeout
	}
	// delete the notify chan
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, seqId int) bool {
	if seq, ok := kv.client2Seq[clientId]; ok {
		return seqId == seq
	} else {
		return false
	}
}

func (kv *ShardKV) Shard(args *ShardArgs, reply *ShardReply) {
	kv.mu.Lock()
	// if server isn't leader, reply err
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Group%dServer%d isn't leader! Reply errWrongLeader.", kv.gid, kv.me)
		return
	}
	// if group isn't the owner of shard, reject it
	if kv.config.Shards[args.ShardIdx] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("Group%d isn't the owner of shard%d! Reply errWrongGroup.", kv.gid, args.ShardIdx)
		return
	}
	// if shard is old, reply ok
	if args.ConfigNum < kv.config.Num || (args.ConfigNum == kv.config.Num && kv.shardDBs[args.ShardIdx].State == Serving) {
		reply.Err = OK
		kv.mu.Unlock()
		DPrintf("Group%dServer%d configNum is %d, shardState is %v, args.configNum is %d.", kv.gid, kv.me, kv.config.Num, kv.shardDBs[args.ShardIdx].State, args.ConfigNum)
		return
	} else if args.ConfigNum > kv.config.Num {
		// if config is new, reject it
		reply.Err = ErrWrongConfigNum
		kv.mu.Unlock()
		DPrintf("Group%dServer%d configNum is %d, shardState is %v, args.configNum is %d.", kv.gid, kv.me, kv.config.Num, kv.shardDBs[args.ShardIdx].State, args.ConfigNum)
		return
	}

	// try to log the shard
	shard := make(map[string]string)
	for key, value := range args.Shard {
		shard[key] = value
	}
	client2Seq := make(map[int64]int)
	for key, value := range args.Client2Seq {
		client2Seq[key] = value
	}
	index, _, isLeader := kv.rf.Start(Op{
		Command: opShard,
		Shard: ShardInfo{
			Num:        args.ConfigNum,
			Idx:        args.ShardIdx,
			Shard:      shard,
			Client2Seq: client2Seq,
		},
	})
	DPrintf("Group%dServer%d add Log%d:{Shard, num:%d, idx:%d, shard:%v}", kv.gid, kv.me, index, args.ConfigNum, args.ShardIdx, shard)
	// to determine whether it is the leader again
	if !isLeader {
		DPrintf("Group%dServer%d isn't leader! Reply errWrongLeader.", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// create the notify chan
	ch := kv.makeNotifyChan(index)
	kv.mu.Unlock()
	// wait for notify chan
	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("Group%dServer%d execute Timeout!", kv.gid, kv.me)
		reply.Err = ErrExecuteTimeout
	}
	DPrintf("Group%dServer%d reply is %v.", kv.gid, kv.me, reply)
	// delete the notify chan
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) makeNotifyChan(index int) chan ApplyRes {
	kv.notifyChans[index] = make(chan ApplyRes)
	return kv.notifyChans[index]
}

func (kv *ShardKV) getNotifyChan(index int) chan ApplyRes {
	if _, ok := kv.notifyChans[index]; ok {
		return kv.notifyChans[index]
	} else {
		return nil
	}
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
	kv.shardDBs = make([]ShardDB, shardctrler.NShards)
	kv.notifyChans = make(map[int]chan ApplyRes)
	kv.client2Seq = make(map[int64]int)

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
			DPrintf("Group%dServer%d try to apply Log%d, Log: %v.", kv.gid, kv.me, msg.CommandIndex, msg.Command.(Op))
			// if log already apply, discard it
			if msg.CommandIndex <= kv.lastApplied {
				DPrintf("Group%dServer%d has already apply the Log%d, discard it.", kv.gid, kv.me, msg.CommandIndex)
				kv.mu.Unlock()
				continue
			}

			// apply log and save the result
			var applyRes ApplyRes
			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			switch op.Command {
			case opConfig:
				// if it't config update log, apply it
				kv.applyConfig(op.Config)
			case opShard:
				applyRes = kv.receivShard(op.Shard)
			case opShardClear:
				kv.clearShard(op.Shard)
			case opGet:
				fallthrough
			case opAppend:
				fallthrough
			case opPut:
				if kv.isDuplicateRequest(op.ClientId, op.SeqId) {
					DPrintf("Group%dServer%d doesn't apply Log%d, because it's duplicate.", kv.gid, kv.me, msg.CommandIndex)
					applyRes.Err = OK
				} else {
					applyRes = kv.applyLogToDB(op)
					kv.client2Seq[op.ClientId] = op.SeqId
				}
			}

			// if server is leader, reply to client
			if _, isLeader := kv.rf.GetState(); isLeader {
				if ch := kv.getNotifyChan(msg.CommandIndex); ch != nil {
					ch <- applyRes
				}
			}
			// if logSize is overLength, snapshot and cut the log
			if needSnapshot := kv.needSnapshot(); needSnapshot {
				kv.snapshot(msg.CommandIndex)
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			DPrintf("Group%dServer%d try to apply snapshot throught index%d.", kv.gid, kv.me, msg.SnapshotIndex)
			kv.applySnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
		} else {
			log.Fatalf("Invaild apply log: %v.", msg)
		}
	}
}

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(&kv.shardDBs); err != nil {
		DPrintf("Group%dServer%d can not encode the kvShardDBS.", kv.gid, kv.me)
	}

	if err := e.Encode(&kv.config); err != nil {
		DPrintf("Group%dServer%d can not encode the kvShardDBS.", kv.gid, kv.me)
	}

	if err := e.Encode(&kv.client2Seq); err != nil {
		DPrintf("Group%dServer%d can not encode the client2Seq.", kv.gid, kv.me)
	}

	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.shardDBs); err != nil {
		DPrintf("Group%dServer%d can not decode the kvShardDBS.", kv.gid, kv.me)
	}

	kv.config = shardctrler.Config{}
	if err := d.Decode(&kv.config); err != nil {
		DPrintf("Group%dServer%d can not decode the config.", kv.gid, kv.me)
	}

	if err := d.Decode(&kv.client2Seq); err != nil {
		DPrintf("Group%dServer%d can not decode the client2Seq.", kv.gid, kv.me)
	}
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	if kv.maxraftstate <= kv.rf.GetRaftStateSize() {
		return true
	}

	return false
}

func (kv *ShardKV) applyLogToDB(op Op) ApplyRes {
	applyRes := ApplyRes{
		ClientId: op.ClientId,
		SeqId:    op.SeqId,
	}

	switch op.Command {
	case opGet:
		applyRes.Value, applyRes.Err = kv.get(op.Key)
		if applyRes.Err == OK {
			DPrintf("Group%dServer%d Apply %v to shardDB, key is %v, value is %v.", kv.gid, kv.me, op.Command, op.Key, applyRes.Value)
		}
	case opPut:
		applyRes.Err = kv.put(op.Key, op.Value)
		if applyRes.Err == OK {
			DPrintf("Group%dServer%d Apply %v to shardDB, key is %v, value is %v.", kv.gid, kv.me, op.Command, op.Key, op.Value)
		}
	case opAppend:
		applyRes.Err = kv.append(op.Key, op.Value)
		if applyRes.Err == OK {
			DPrintf("Group%dServer%d Apply %v to shardDB, key is %v, value is %v.", kv.gid, kv.me, op.Command, op.Key, op.Value)
		}
	}
	return applyRes
}

func (kv *ShardKV) get(key string) (string, Err) {
	idx := key2shard(key)
	// if kv.config.Shards[idx] != kv.gid {
	// 	return "", ErrWrongOwner
	// }
	// if kv.shardDBs[idx].State != Serving {
	// 	return "", ErrPauseServe
	// }

	if value, ok := kv.shardDBs[idx].DB[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *ShardKV) put(key, value string) Err {
	idx := key2shard(key)
	// if kv.config.Shards[idx] != kv.gid {
	// 	return ErrWrongOwner
	// }
	// if kv.shardDBs[idx].State != Serving {
	// 	return ErrPauseServe
	// }

	kv.shardDBs[idx].DB[key] = value
	return OK
}

func (kv *ShardKV) append(key, value string) Err {
	idx := key2shard(key)
	// if kv.config.Shards[idx] != kv.gid {
	// 	return ErrWrongOwner
	// }
	// if kv.shardDBs[idx].State != Serving {
	// 	return ErrPauseServe
	// }

	kv.shardDBs[idx].DB[key] += value
	return OK
}

func (kv *ShardKV) fetchConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()

		if kv.isLeader() && !kv.isReConfiguring() {
			configNum := kv.config.Num
			kv.mu.Unlock()

			newConfig := kv.scClerk.Query(configNum + 1)
			// if config is new, update config
			if newConfig.Num > configNum {
				DPrintf("Group%dServer%d update config%d -> %d, newShard: %v", kv.gid, kv.me, configNum, newConfig.Num, newConfig.Shards)
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
		if kv.isLeader() {
			for idx, shardDB := range kv.shardDBs {
				if shardDB.State == Pushing {
					// try to send shard to new owner
					args := ShardArgs{
						ConfigNum:  kv.config.Num,
						ShardIdx:   idx,
						Shard:      make(map[string]string),
						Client2Seq: make(map[int64]int),
					}
					for key, value := range shardDB.DB {
						args.Shard[key] = value
					}
					for key, value := range kv.client2Seq {
						args.Client2Seq[key] = value
					}

					gid := kv.config.Shards[idx]
					DPrintf("Group%dServer%d send shard%d to Group%d in config%d, shard%v.", kv.gid, kv.me, idx, gid, kv.config.Num, shardDB.DB)
					if servers, ok := kv.config.Groups[gid]; ok {
						// try each server for the shard.
						for si := 0; si < len(servers); si = (si + 1) % len(servers) {
							srv := kv.make_end(servers[si])
							var reply ShardReply
							ok := srv.Call("ShardKV.Shard", &args, &reply)
							if ok && reply.Err == OK {
								// After send shard to new own, clear the shard
								index, _, _ := kv.rf.Start(Op{
									Command: opShardClear,
									Shard: ShardInfo{
										Num:   args.ConfigNum,
										Idx:   args.ShardIdx,
										Shard: nil,
									},
								})
								DPrintf("Group%dServer%d add Log%d:{ShardClear, num:%d, idx:%d}", kv.gid, kv.me, index, args.ConfigNum, args.ShardIdx)
								break
							}
						}
					}
				}
			}
		}

		kv.mu.Unlock()
		time.Sleep(time.Duration(MigrateInterval) * time.Millisecond)
	}
}

func (kv *ShardKV) receivShard(shard ShardInfo) ApplyRes {
	applyRes := ApplyRes{
		Err: OK,
	}

	if shard.Num > kv.config.Num {
		applyRes.Err = ErrWrongConfigNum
	} else if shard.Num == kv.config.Num {
		if kv.shardDBs[shard.Idx].State == Pulling {
			kv.shardDBs[shard.Idx].DB = make(map[string]string)
			for key, value := range shard.Shard {
				kv.shardDBs[shard.Idx].DB[key] = value
			}
			for key, value := range shard.Client2Seq {
				if oldValue, ok := kv.client2Seq[key]; !ok || oldValue < value {
					kv.client2Seq[key] = value
				}
			}
			kv.shardDBs[shard.Idx].State = Serving
		} else if kv.shardDBs[shard.Idx].State == Pushing {
			applyRes.Err = ErrWrongOwner
		}
	}

	return applyRes
}

func (kv *ShardKV) clearShard(shard ShardInfo) {
	if shard.Num != kv.config.Num || kv.shardDBs[shard.Idx].State != Pushing {
		return
	}

	kv.shardDBs[shard.Idx] = ShardDB{
		State: Invalid,
		DB:    make(map[string]string),
	}
}

func (kv *ShardKV) applyConfig(newConfig shardctrler.Config) {
	// update config
	kv.config.Num = newConfig.Num
	kv.config.Shards = newConfig.Shards
	kv.config.Groups = make(map[int][]string)
	for gid, servers := range newConfig.Groups {
		kv.config.Groups[gid] = append([]string{}, servers...)
	}
	// update shard state
	if newConfig.Num == InitConfigNum {
		for i, gid := range newConfig.Shards {
			if gid == kv.gid {
				kv.shardDBs[i].State = Serving
				kv.shardDBs[i].DB = make(map[string]string)
			}
		}
	} else {
		for i, gid := range newConfig.Shards {
			if gid == kv.gid {
				// wait for receving shard
				if kv.shardDBs[i].State == Invalid {
					kv.shardDBs[i].State = Pulling
					kv.shardDBs[i].DB = make(map[string]string)
				}
			} else {
				// set pushing, and the send shard to new owner
				if kv.shardDBs[i].State == Serving {
					kv.shardDBs[i].State = Pushing
				}
			}
		}
	}
	DPrintf("Group%dServer%d update to Config%d.", kv.gid, kv.me, kv.config.Num)
}

func (kv *ShardKV) updateConfig(newConfig shardctrler.Config) {
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
	index, _, _ := kv.rf.Start(command)
	DPrintf("Group%dServer%d add Log%d:{Config, num:%d, shards:%v, group:%v}", kv.gid, kv.me, index, newConfig.Num, newConfig.Shards, newConfig.Groups)
}

func (kv *ShardKV) isReConfiguring() bool {
	// if any shard is at pulling state or pushing state, that mean reconfiguration doesn't finish
	for _, shardDB := range kv.shardDBs {
		if shardDB.State == Pulling || shardDB.State == Pushing {
			return true
		}
	}
	return false
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}
