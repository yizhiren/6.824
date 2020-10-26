package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync/atomic"
//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	requestId int64
	clientId int64
	dead      int32
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
	// Your code here.

	ck.requestId = nrand()
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Kill() {
	atomic.StoreInt32(&ck.dead, 1)
}

func (ck *Clerk) killed() bool {
	z := atomic.LoadInt32(&ck.dead)
	return z == 1
}

func (ck *Clerk) UpdateRequestId() {
	newRequestId := nrand()
	for ck.requestId == newRequestId {
		newRequestId = nrand()
	}
	ck.requestId = newRequestId
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.UpdateRequestId()
				return reply.Config
			}
			//if !ok {
				//fmt.Printf("sm.query.NOT OK,%v,%+v,%+v\n", ok, args, reply)
			//}
		}
		if ck.killed() {
			return Config{}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.Servers = servers
	DPrintf("clerk.Join, servers:%+v\n", servers)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.UpdateRequestId()
				return
			}
		}
		if ck.killed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.UpdateRequestId()
				return
			}
		}
		if ck.killed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.UpdateRequestId()
				return
			}
		}
		if ck.killed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
