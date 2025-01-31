package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seqId    int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.seqId++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				DPrintf("Client%d send Get request to Group%d Server%d. SeqId is %d, key is %v.", ck.clientId, gid, si, ck.seqId, key)
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK || reply.Err == ErrNoKey {
						DPrintf("Client%d received reply: %v.", ck.clientId, reply)
						return reply.Value
					} else if reply.Err == ErrWrongGroup || reply.Err == ErrWrongOwner || reply.Err == ErrPauseServe {
						DPrintf("Client%d received reply: %v.", ck.clientId, reply.Err)
						break
					} else if reply.Err == ErrWrongLeader || reply.Err == ErrExecuteTimeout { // ... not ok, or ErrWrongLeader
						DPrintf("Client%d received reply: %v.", ck.clientId, reply.Err)
						continue
					} else {
						DPrintf("Client%d received reply: %v.", ck.clientId, reply.Err)
					}
				} else {
					DPrintf("Client%d can't received reply!", ck.clientId)
				}

			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqId++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				DPrintf("Client%d send %v request to Group%d Server%d. SeqId is %d, key is %v, value is %v.", ck.clientId, op, gid, si, ck.seqId, key, value)
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						DPrintf("Client%d received reply: %v.", ck.clientId, reply)
						return
					} else if reply.Err == ErrWrongGroup || reply.Err == ErrWrongOwner || reply.Err == ErrPauseServe {
						DPrintf("Client%d received reply: %v.", ck.clientId, reply.Err)
						break
					} else if reply.Err == ErrWrongLeader || reply.Err == ErrExecuteTimeout { // ... not ok, or ErrWrongLeader
						DPrintf("Client%d received reply: %v.", ck.clientId, reply.Err)
						continue
					} else {
						DPrintf("Client%d received reply: %v.", ck.clientId, reply.Err)
					}
				} else {
					DPrintf("Client%d can't received reply!", ck.clientId)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
