package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ExecuteTimeout int64 = 1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Command  string
	ClientId int64
	SeqId    int
}

type ApplyRes struct {
	err      Err
	value    string
	clientId int64
	seqId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int
	kvBD        map[string]string
	notifyChans map[int]chan ApplyRes
	lastOpRes   map[int64]ApplyRes
}

func (kv *KVServer) get(key string) (string, Err) {
	if value, ok := kv.kvBD[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KVServer) put(key, value string) Err {
	kv.kvBD[key] = value
	return OK
}

func (kv *KVServer) append(key, value string) Err {
	kv.kvBD[key] += value
	return OK
}

func (kv *KVServer) isDuplicateRequest(clientId int64, seqId int) bool {
	if res, ok := kv.lastOpRes[clientId]; ok {
		return seqId == res.seqId
	} else {
		return false
	}
}

func (kv *KVServer) makeNotifyChan(index int) chan ApplyRes {
	kv.notifyChans[index] = make(chan ApplyRes)
	return kv.notifyChans[index]
}

func (kv *KVServer) getNotifyChan(index int) chan ApplyRes {
	if _, ok := kv.notifyChans[index]; ok {
		return kv.notifyChans[index]
	} else {
		return nil
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// if server isn't leader, reply err
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Server %d isn't leader! Reply errWrongLeader.", kv.rf.GetMe())
		return
	}
	// if request is duplicate, reply the last result
	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		reply.Err = kv.lastOpRes[args.ClientId].err
		reply.Value = kv.lastOpRes[args.ClientId].value
		kv.mu.Unlock()
		DPrintf("DuplicateRequest! Reply the lastOpRes: err is %v.", reply)
		return
	}
	// try to log the request
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Command:  opGet,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})
	// to determine whether it is the leader again
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Server %d isn't leader! Reply errWrongLeader.", kv.rf.GetMe())
		return
	}
	// create the notify chan
	ch := kv.makeNotifyChan(index)
	kv.mu.Unlock()
	// wait for result
	select {
	case result := <-ch:
		reply.Err, reply.Value = result.err, result.value
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("Execute Timeout!")
		reply.Err = ErrExecuteTimeout
	}
	DPrintf("Reply is %v.", reply)
	// delete the notify chan
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d Receive %v request from %d, key is %v, value is %v.", kv.rf.GetMe(), args.Op, args.ClientId, args.Key, args.Value)
	kv.mu.Lock()
	// if server isn't leader, reply err
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Server %d isn't leader! Reply errWrongLeader.", kv.rf.GetMe())
		return
	}
	// if request is duplicate, reply the last result
	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		reply.Err = kv.lastOpRes[args.ClientId].err
		kv.mu.Unlock()
		DPrintf("DuplicateRequest! Reply the lastOpRes: err is %v.", reply)
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
	// to determine whether it is the leader again
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("Server %d isn't leader! Reply errWrongLeader.", kv.rf.GetMe())
		return
	}
	// create the notify chan
	ch := kv.makeNotifyChan(index)
	kv.mu.Unlock()
	// wait for result
	select {
	case result := <-ch:
		reply.Err = result.err
	case <-time.After(time.Duration(ExecuteTimeout) * time.Second):
		DPrintf("Execute Timeout!")
		reply.Err = ErrExecuteTimeout
	}

	DPrintf("Reply is %v.", reply)
	// delete the notify chan
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) applyLogToDB(op Op) ApplyRes {
	applyRes := ApplyRes{
		clientId: op.ClientId,
		seqId:    op.SeqId,
	}

	switch op.Command {
	case opGet:
		applyRes.value, applyRes.err = kv.get(op.Key)
		DPrintf("Server %d Apply %v to kvDB, key is %v.", kv.rf.GetMe(), op.Command, op.Key)
	case opPut:
		applyRes.err = kv.put(op.Key, op.Value)
		DPrintf("Server %d Apply %v to kvDB, key is %v, value is %v.", kv.rf.GetMe(), op.Command, op.Key, op.Value)
	case opAppend:
		applyRes.err = kv.append(op.Key, op.Value)
		DPrintf("Server %d Apply %v to kvDB, key is %v, value is %v.", kv.rf.GetMe(), op.Command, op.Key, op.Value)
	}
	return applyRes
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			// if log already apply, discard it
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				DPrintf("Log has already apply, discard it.")
				continue
			}

			// if log doesn't apply, apply it and save the result
			var applyRes ApplyRes
			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			if kv.isDuplicateRequest(op.ClientId, op.SeqId) {
				applyRes = kv.lastOpRes[op.ClientId]
			} else {
				applyRes = kv.applyLogToDB(op)
				kv.lastOpRes[op.ClientId] = applyRes
			}

			// if server is leader, reply to client
			if _, isLeader := kv.rf.GetState(); isLeader {
				if ch := kv.getNotifyChan(msg.CommandIndex); ch != nil {
					ch <- applyRes
				}
			}

			kv.mu.Unlock()
		} else {
			log.Fatalf("Invaild apply log: %v.", msg)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.kvBD = make(map[string]string)
	kv.notifyChans = make(map[int]chan ApplyRes)
	kv.lastOpRes = make(map[int64]ApplyRes)

	go kv.applier()

	return kv
}
