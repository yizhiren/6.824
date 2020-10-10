package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	requestId int64
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	id, _ := rand.Int(rand.Reader, max)
	x := id.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.lastLeader = 0
	ck.requestId = nrand()
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) RaftKVGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) RaftKVPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) UpdateRequestId() {
	newRequestId := nrand()
	for ck.requestId == newRequestId {
		newRequestId = nrand()
	}
	ck.requestId = newRequestId
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		RequestId: ck.requestId,
		ClientId: ck.clientId,
	}

	cnt := len(ck.servers)
	for {
		reply := GetReply{}
		ok := ck.RaftKVGet(ck.lastLeader, &args, &reply)
		if !ok {
			DPrintf("clerk get, not ok, [%s], lastLeader=%d", key, ck.lastLeader)
			ck.lastLeader = (ck.lastLeader + 1) % cnt
			continue
		}
		if reply.Err == ErrNotLeader {
			DPrintf("clerk get, not leader, [%s], lastLeader=%d", key, ck.lastLeader)
			ck.lastLeader = (ck.lastLeader + 1) % cnt
			continue
		}

		ck.UpdateRequestId()
		return reply.Value
	}


	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		RequestId: ck.requestId,
		ClientId: ck.clientId,
	}

	cnt := len(ck.servers)
	for {
		reply := PutAppendReply{}
		ok := ck.RaftKVPutAppend(ck.lastLeader, &args, &reply)
		if !ok {
			DPrintf("clerk %s, not ok, [%s]%s, lastLeader=%d", op, key, value, ck.lastLeader)
			ck.lastLeader = (ck.lastLeader + 1) % cnt
			continue
		}
		if reply.Err == ErrNotLeader {
			DPrintf("clerk %s, not leader, [%s]%s, lastLeader=%d", op, key, value, ck.lastLeader)
			ck.lastLeader = (ck.lastLeader + 1) % cnt
			continue
		}

		ck.UpdateRequestId()

		return
	}


	// You will have to modify this function.
	return 
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
