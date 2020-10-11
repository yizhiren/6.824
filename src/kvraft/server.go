package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"bytes"
	//"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key string
	Value string
	RequestId int64
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastSnapshotIndex int
	snapshoting bool

	// Your definitions here.
	Kvmap map[string]string
	Duplicate map[int64]map[string]int64 // [clientId]([key]requestId)
	RequestHandlers map[int]chan raft.ApplyMsg
}

func (kv *KVServer) registerIndexHandler(index int) chan raft.ApplyMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.RequestHandlers[index] = awaitChan

	return awaitChan
}

// to be update
func (kv *KVServer) await(index int, term int, op Op) (success bool) {
	awaitChan := kv.registerIndexHandler(index)

	for {
		select {
		case message := <-awaitChan:
			if kv.RaftBecomeFollower(&message) {
				return false
			}
			if (message.CommandValid == true) &&
				(index == message.CommandIndex) {
			 	return (term == message.CommandTerm)
			}
			// continue
		}
	}
}

func (kv *KVServer) GetKeyValue(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.Kvmap[key]
	return val,ok
}

func (kv *KVServer) GetDuplicate(clientId int64, key string) (int64, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientRequest, haveClient := kv.Duplicate[clientId]
	if !haveClient {
		return 0,false
	}
	val, ok := clientRequest[key]
	return val, ok
}

func (kv *KVServer) SetDuplicateNolock(clientId int64, key string, requestId int64)  {
	_, haveClient := kv.Duplicate[clientId]
	if !haveClient {
		kv.Duplicate[clientId] = make(map[string]int64)
	}
	kv.Duplicate[clientId][key] = requestId
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	ops := Op {
		Method: "Get",
		Key: args.Key,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

	index, term, isLeader := kv.rf.Start(ops)

	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	success := kv.await(index, term, ops)
	if !success {
		reply.Err = ErrNotLeader
		return
	} else {	
		val, ok := kv.GetKeyValue(args.Key)
		if ok {
			reply.Value = val
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	ops := Op {
		Method: args.Op,
		Key: args.Key,
		Value: args.Value,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

	dup, ok := kv.GetDuplicate(ops.ClientId, ops.Key)
	if ok && (dup == ops.RequestId) {
		reply.Err = OK
		return
	}

	index, term, isLeader := kv.rf.Start(ops)

	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	success := kv.await(index, term, ops)
	if !success {
		reply.Err = ErrNotLeader
		return
	} else {	
		reply.Err = OK
		return
	}
}

func (kv *KVServer) OnApplyEntry(m *raft.ApplyMsg) {
	ops := m.Command.(Op)
	dup, ok := kv.GetDuplicate(ops.ClientId, ops.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !ok || (dup != ops.RequestId) {
		// save the client id and its serial number
		switch ops.Method {
		case "Get":
			// nothing
		case "Put":
			kv.Kvmap[ops.Key] = ops.Value
			kv.SetDuplicateNolock(ops.ClientId, ops.Key, ops.RequestId)
		case "Append":
			kv.Kvmap[ops.Key] += ops.Value
			kv.SetDuplicateNolock(ops.ClientId, ops.Key, ops.RequestId)
		}
	}

	ch, ok := kv.RequestHandlers[m.CommandIndex]
	if ok {
		delete(kv.RequestHandlers, m.CommandIndex)
		ch <- *m
	}
}

func (kv *KVServer) RaftBecomeFollower(m *raft.ApplyMsg) bool {
	return (m.CommandValid == false) && 
		(m.Type == raft.MsgTypeRole) &&
		(m.Role == raft.RoleFollower)
}

func (kv *KVServer) OnRoleNotify(m *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.RaftBecomeFollower(m) {
	    for index, ch := range kv.RequestHandlers {
			delete(kv.RequestHandlers, index)
			ch <- *m
	    }	
	}

}


func (kv *KVServer) OnSnapshot(m *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.loadSnapshot(m.Snapshot)
}

func (kv *KVServer) DoSnapshot(lastIncludedIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if lastIncludedIndex <= kv.lastSnapshotIndex {
		return
	}
	kv.lastSnapshotIndex = lastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Kvmap)
	e.Encode(kv.lastSnapshotIndex)
	e.Encode(kv.Duplicate)
	snapshot := w.Bytes()

	kv.snapshoting = true
	go func() {
		kv.rf.PersistStateAndSnapshot(kv.lastSnapshotIndex, snapshot)
		kv.snapshoting = false
	}()
}

func (kv *KVServer) SnapshotIfNeeded(index int) {
	if kv.maxraftstate > 0 && !kv.snapshoting {
		var threshold = int(0.95 * float64(kv.maxraftstate))
		if kv.rf.GetRaftStateSize() > threshold {
			kv.DoSnapshot(index)
		}
	}

}

func (kv *KVServer) receivingApplyMsg() {
	for {
		select {
		case m := <-kv.applyCh:
				if m.CommandValid {
					DPrintf("periodCheckApplyMsg receive entry message. %+v.", m)
					kv.OnApplyEntry(&m)
					kv.SnapshotIfNeeded(m.CommandIndex)
				} else if(m.Type == raft.MsgTypeKill) {
					DPrintf("periodCheckApplyMsg receive kill message. %+v.", m)
					return 
				} else if(m.Type == raft.MsgTypeRole) {
					DPrintf("periodCheckApplyMsg receive role message. %+v.", m)
					kv.OnRoleNotify(&m)
					
				} else if(m.Type == raft.MsgTypeSnapshot) {
					DPrintf("periodCheckApplyMsg receive snapshot message. %+v.", m)
					kv.OnSnapshot(&m)
				}
		}

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) loadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kvmap := make(map[string]string)
	snapshotIndex := 0
	duplicate := make(map[int64]map[string]int64)

	d.Decode(&kvmap)
	d.Decode(&snapshotIndex)
	d.Decode(&duplicate)

	kv.Kvmap = kvmap
	kv.lastSnapshotIndex = snapshotIndex
	kv.Duplicate = duplicate
	
}

//
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
//
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
	kv.Kvmap = make(map[string]string)
	kv.Duplicate = make(map[int64]map[string]int64)
	kv.RequestHandlers = make(map[int]chan raft.ApplyMsg)

	if kv.maxraftstate > 0 {
		data := persister.ReadSnapshot()
		if len(data) > 0 {
			kv.loadSnapshot(data)
		}
	}
	DPrintf("kv server %d start, value=%+v\n", kv.me, kv)
	// You may need initialization code here.

	go kv.receivingApplyMsg()
	return kv
}
