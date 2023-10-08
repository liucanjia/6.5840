package shardkv

import (
	"log"

	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrExecuteTimeout = "ErrExecuteTimeout"
	ErrWrongOwner     = "ErrWrongOwner"
	ErrPauseServe     = "ErrPauseServe"
	ErrWrongConfigNum = "ErrWrongConfigNum"

	opGet        = "opGet"
	opPut        = "Put"
	opAppend     = "Append"
	opConfig     = "opConfig"
	opShard      = "opShard"
	opShardClear = "opShardClear"
)

const Debug = false

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardArgs struct {
	ConfigNum int
	ShardIdx  int
	Shard     map[string]string
}

type ShardReply struct {
	Err Err
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
