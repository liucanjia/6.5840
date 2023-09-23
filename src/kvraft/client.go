package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int64
	seqId    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) updateLeader(newLeaderId int) {
	if newLeaderId != ck.leaderId {
		// if leader changed, save new leaderId
		ck.leaderId = newLeaderId
		DPrintf("leader has changed, save new leaderId %d.", ck.leaderId)
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	ck.sendGet(key, &value)
	return value
}

func (ck *Clerk) sendGet(key string, value *string) {
	serverCnt := len(ck.servers)
	leaderId := ck.leaderId
	ck.seqId++

	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := &GetReply{}
		DPrintf("Client %d send Get request to Server %d: seqId is %d, key is %v.", args.ClientId, leaderId, args.SeqId, key)
		ok := ck.servers[leaderId].Call("KVServer.Get", args, reply)

		if ok {
			switch reply.Err {
			case OK:
				*value = reply.Value
				DPrintf("Client %d get reply for the Get request, key is %v, value is %v.", args.ClientId, args.Key, reply.Value)
				ck.updateLeader(leaderId)
				return
			case ErrNoKey:
				DPrintf("Client %d Get request key is invaild.", args.ClientId)
				ck.updateLeader(leaderId)
				return
			case ErrWrongLeader:
				DPrintf("Client %d get reply for the Get request: %v.", args.ClientId, reply.Err)
				leaderId = (leaderId + 1) % serverCnt
			case ErrExecuteTimeout:
				DPrintf("Client %d Get request executing timeout, retry.", args.ClientId)
				leaderId = (leaderId + 1) % serverCnt
			}
		} else {
			DPrintf("Client %d cann't connet with Server %d.", args.ClientId, leaderId)
			leaderId = (leaderId + 1) % serverCnt
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sendPutAppend(key, value, op)
}

func (ck *Clerk) sendPutAppend(key string, value string, op string) {
	serverCnt := len(ck.servers)
	leaderId := ck.leaderId
	ck.seqId++
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := &PutAppendReply{}
		DPrintf("Client %d send %v request to Server %d: seqId is %d, key is %v, value is %v.", args.ClientId, op, leaderId, args.SeqId, key, value)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)

		if ok {
			switch reply.Err {
			case OK:
				ck.updateLeader(leaderId)
				return
			case ErrNoKey:
				// impossible
				return
			case ErrWrongLeader:
				leaderId = (leaderId + 1) % serverCnt
			case ErrExecuteTimeout:
				DPrintf("Client %d PutAppend request executing timeout, retry.", args.ClientId)
				leaderId = (leaderId + 1) % serverCnt
			}
		} else {
			DPrintf("Client %d cann't connet with Server %d.", args.ClientId, leaderId)
			leaderId = (leaderId + 1) % serverCnt
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, opPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, opAppend)
}
