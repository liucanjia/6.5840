package shardctrler

import "log"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const Debug = false

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK                = "OK"
	ErrExecuteTimeout = "ErrExecuteTimeout"

	opJoin  = "Join"
	opLeave = "Leave"
	opMove  = "Move"
	opQuery = "Query"
)

type Err string

type ClientInfo struct {
	ClientId int64
	SeqId    int
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Client  ClientInfo
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs   []int
	Client ClientInfo
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard  int
	GID    int
	Client ClientInfo
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num    int // desired config number
	Client ClientInfo
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
