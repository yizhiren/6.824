package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrNotLeader   = "ErrNotLeader"
	ErrRetry       = "ErrRetry"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardArgs struct {
	SrcGid int
	DstGid int
	DstConfigVersion int
	ShardNumber int
	Kvmap map[string]string
	Duplicate map[int64]map[string]int64
}

type MigrateShardReply struct {
	WrongLeader bool
	Err         Err
}

