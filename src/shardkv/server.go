package shardkv


import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "time"
import "log"
import "fmt"
import "bytes"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func LockPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == -1 {
		log.Printf(format, a...)
	}
	return
}

const ShardMasterCheckInterval = 20 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key string
	Value string
	RequestId int64
	ClientId int64

	ApplyConfig shardmaster.Config
	MigrateShard MigrateShardArgs
}

type ShardStatus int
const (
	AVALIABLE = 1
	EXPORTING = 2
	IMPORTING = 3
	NOTOWNED = 4

	FINISH_EXPORTING = -2
	FINISH_IMPORTING = -3

	BUSY_EXPORTING = -4
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm       *shardmaster.Clerk
	lastSnapshotIndex int
	snapshoting bool

	Kvmap [shardmaster.NShards]map[string]string
	Duplicate [shardmaster.NShards]map[int64]map[string]int64 // [clientId]([key]requestId)
	RequestHandlers map[int]chan raft.ApplyMsg

	CurShardStatus [shardmaster.NShards]ShardStatus
	AppliedShardConfig shardmaster.Config
	MasterShardConfig shardmaster.Config

	shutdown chan struct{}
}

func (kv *ShardKV) registerIndexHandler(index int) chan raft.ApplyMsg {
	//fmt.Printf("gid(%d) me(%d), leader(%t) registerIndexHandler.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			
	kv.mu.Lock()
	//fmt.Printf("gid(%d) me(%d), leader(%t) registerIndexHandler.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	defer kv.mu.Unlock()

	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.RequestHandlers[index] = awaitChan

	//fmt.Printf("gid(%d) me(%d), leader(%t) registerIndexHandler.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	return awaitChan
}

// to be update
func (kv *ShardKV) await(index int, term int, op Op) (success bool) {
	awaitChan := kv.registerIndexHandler(index)

	for {
		select {
		case message := <-awaitChan:
			if kv.RaftBecomeFollower(&message) {
				return false
			}
			if (message.CommandValid == true) &&
				(index == message.CommandIndex) {

				// under the lock of onApplyEntry
				isStillOwnTheKey := kv.IsOwnThisKeyNolock(op.Key)
			 	return isStillOwnTheKey && (term == message.CommandTerm)
			}
			// continue
		}
	}
}

func (kv *ShardKV) GetKeyValue(key string) (string, bool) {
		//fmt.Printf("gid(%d) me(%d), leader(%t) GetKeyValue.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
			//fmt.Printf("gid(%d) me(%d), leader(%t) GetKeyValue.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	defer kv.mu.Unlock()
	
	shardNum := key2shard(key)
	val, ok := kv.Kvmap[shardNum][key]

			//fmt.Printf("gid(%d) me(%d), leader(%t) GetKeyValue.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	return val,ok
}

func (kv *ShardKV) GetDuplicateNolock(clientId int64, key string) (int64, bool) {

	shardNum := key2shard(key)
	clientRequest, haveClient := kv.Duplicate[shardNum][clientId]
	if !haveClient {
		return 0,false
	}
	val, ok := clientRequest[key]
	return val, ok
}

func (kv *ShardKV) SetDuplicateNolock(clientId int64, key string, requestId int64)  {
	shardNum := key2shard(key)
	_, haveClient := kv.Duplicate[shardNum][clientId]
	if !haveClient {
		kv.Duplicate[shardNum][clientId] = make(map[string]int64)
	}
	kv.Duplicate[shardNum][clientId][key] = requestId
}

func (kv *ShardKV) IsOwnThisKeyNolock(key string) bool {

	shardNum := key2shard(key)
	status := kv.CurShardStatus[shardNum]
	return (AVALIABLE == status) || (FINISH_IMPORTING == status)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

			//fmt.Printf("gid(%d) me(%d), leader(%t) Get.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
			//fmt.Printf("gid(%d) me(%d), leader(%t) Get.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	if !kv.IsOwnThisKeyNolock(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}


	ops := Op {
		Method: "Get",
		Key: args.Key,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

			//fmt.Printf("gid(%d) me(%d), leader(%t) Get.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Unlock()

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	//fmt.Printf("gid(%d) me(%d), leader(%t) PutAppend.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
	//fmt.Printf("gid(%d) me(%d), leader(%t) PutAppend.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	if !kv.IsOwnThisKeyNolock(args.Key) {
		//fmt.Printf("gid(%d) me(%d), leader(%t) PutAppend.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
		
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	ops := Op {
		Method: args.Op,
		Key: args.Key,
		Value: args.Value,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

	dup, ok := kv.GetDuplicateNolock(ops.ClientId, ops.Key)
	if ok && (dup == ops.RequestId) {
		//fmt.Printf("gid(%d) me(%d), leader(%t) PutAppend.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	//fmt.Printf("gid(%d) me(%d), leader(%t) PutAppend.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Unlock()

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


func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	//fmt.Printf("gid(%d) me(%d), leader(%t) MigrateShard args %+v\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(),args)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}


		//fmt.Printf("gid(%d) me(%d), leader(%t) MigrateShard.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
		//fmt.Printf("gid(%d) me(%d), leader(%t) MigrateShard.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	defer kv.mu.Unlock()
	//defer 	//fmt.Printf("gid(%d) me(%d), leader(%t) MigrateShard.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	


	if kv.AppliedShardConfig.Num + 1 < args.DstConfigVersion {
		reply.Err = ErrWrongGroup
		return
	}

	if (kv.CurShardStatus[args.ShardNumber] == AVALIABLE) ||
		 (kv.CurShardStatus[args.ShardNumber] == FINISH_IMPORTING) {
		reply.Err = OK
		return
	}

	if kv.CurShardStatus[args.ShardNumber] != IMPORTING {
		reply.Err = ErrWrongGroup
		return
	}

	ops := Op {
		Method : "ImportComplete",
		MigrateShard: *args,
	}

	go kv.rf.Start(ops)

	reply.Err = OK

}

func CloneKv(a, b *map[string]string)  {
	for k,v := range (*a) {
		(*b)[k] = v
	}
}

func CloneDuplicate(a, b *map[int64]map[string]int64)  {
	for clientId, mapping := range (*a) {
		(*b)[clientId] = make(map[string]int64)
		for k, v := range mapping {
			(*b)[clientId][k]=v
		}
	}
}

func CloneConfig(a, b *shardmaster.Config)  {
	b.Num = a.Num
	for i,gid := range a.Shards {
		b.Shards[i] = gid
	}
	for gid := range a.Groups {
		// 假定了一个group中的机器不变
		b.Groups[gid] = a.Groups[gid]
	}
}

func (kv *ShardKV) ExportingShardNolock(shard int, gid int) {

	dstConfigVersion := kv.MasterShardConfig.Num
	destServers := kv.MasterShardConfig.Groups[gid]
	kvmap := map[string]string{}
	duplicate := map[int64]map[string]int64{}
	CloneKv(&kv.Kvmap[shard], &kvmap)
	CloneDuplicate(&kv.Duplicate[shard], &duplicate)

	args := MigrateShardArgs{
		SrcGid: kv.gid,
		DstGid: gid,
		DstConfigVersion: dstConfigVersion,
		ShardNumber: shard,
		Kvmap : kvmap,
		Duplicate : duplicate,
	}
	kv.CurShardStatus[shard] = BUSY_EXPORTING
	//fmt.Printf("gid(%d) me(%d), leader(%t) ExportingShardNolock sending args=%+v\n", kv.gid, kv.me, kv.rf.IsLeaderNolock(), args)
	go func(){
		for si := 0; si < len(destServers); si++ {
			srv := kv.make_end(destServers[si])
			var reply MigrateShardReply
	
			//fmt.Printf("gid(%d) me(%d), leader(%t) ExportingShardNolock R1 %v\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(),destServers[si])
			ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
			//fmt.Printf("gid(%d) me(%d), leader(%t) ExportingShardNolock R2 %v\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(),destServers[si])
			
			if ok && reply.Err == OK {

				ops := Op {
					Method : "ExportComplete",
					MigrateShard: args,
				}
				//fmt.Printf("gid(%d) me(%d), leader(%t) ExportingShardNolock success[+], ops=%+v\n", kv.gid, kv.me, kv.rf.IsLeaderNolock(), ops)
				kv.CurShardStatus[shard] = EXPORTING
				go kv.rf.Start(ops)
				return
			}

			if ok && reply.Err == ErrWrongGroup {
				break
			}

		}
		kv.CurShardStatus[shard] = EXPORTING
		//fmt.Printf("gid(%d) me(%d), leader(%t) ExportingShardNolock sending fail args=%+v\n", kv.gid, kv.me, kv.rf.IsLeaderNolock(), args)

	}()
	

}

func (kv *ShardKV) IsNotFinishBalance() bool {
	for _, status := range kv.CurShardStatus {
		if status == IMPORTING || 
			status == EXPORTING ||
			status == BUSY_EXPORTING {
			return true
		}
	}
	return false
}

func (kv *ShardKV) UpdateAppliedShardConfigIfNeededNolock(dstConfigVersion int) {

	//DPrintf("gid(%d) me(%d), leader(%t) Current Status, %+v\n",
	//	kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.CurShardStatus)

	if dstConfigVersion != kv.MasterShardConfig.Num {
		return
	}
	if kv.IsNotFinishBalance() {
		return 
	}

	kv.AppliedShardConfig = kv.MasterShardConfig
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.CurShardStatus[shard] == FINISH_IMPORTING {
			kv.CurShardStatus[shard] = AVALIABLE
		} 
		if kv.CurShardStatus[shard] == FINISH_EXPORTING {
			kv.CurShardStatus[shard] = NOTOWNED
		} 
	}

	//DPrintf("gid(%d) me(%d), leader(%t) Current Status, %+v, upgrade to version %+v\n",
	//	kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.CurShardStatus, kv.AppliedShardConfig)
}

func (kv *ShardKV) OnImportCompleteNolock(op Op) {

	args := &op.MigrateShard

	if (kv.MasterShardConfig.Num == args.DstConfigVersion) &&
		(kv.CurShardStatus[args.ShardNumber] == IMPORTING) {
		kv.Kvmap[args.ShardNumber] = args.Kvmap
		kv.Duplicate[args.ShardNumber] = args.Duplicate
		kv.CurShardStatus[args.ShardNumber] = FINISH_IMPORTING
		kv.UpdateAppliedShardConfigIfNeededNolock(args.DstConfigVersion)
	} else {
		DPrintf("OnImportComplete fail, kv.CurShardStatus[%d]=%d, args=%+v",
			args.ShardNumber, kv.CurShardStatus[args.ShardNumber], args)
	}

}

func (kv *ShardKV) OnExportCompleteNolock(op Op) {

	args := &op.MigrateShard

	if (kv.MasterShardConfig.Num == args.DstConfigVersion) &&
		(kv.CurShardStatus[args.ShardNumber] == EXPORTING) {
		kv.Kvmap[args.ShardNumber] = make(map[string]string)
		kv.Duplicate[args.ShardNumber] = make(map[int64]map[string]int64)
		kv.CurShardStatus[args.ShardNumber] = FINISH_EXPORTING
		kv.UpdateAppliedShardConfigIfNeededNolock(args.DstConfigVersion)
	} else {
		DPrintf("OnExportComplete fail, kv.CurShardStatus[%d]=%d, args=%+v",
			args.ShardNumber, kv.CurShardStatus[args.ShardNumber], args)
	}

}

func (kv *ShardKV) AlreadyStable() bool {
	if kv.AppliedShardConfig.Num != kv.MasterShardConfig.Num {
		return false
	}

	for shard := 0; shard < shardmaster.NShards; shard++ {
		if (kv.MasterShardConfig.Shards[shard] == kv.gid) && 
			(kv.CurShardStatus[shard] != AVALIABLE) {
			return false
		}
		if (kv.MasterShardConfig.Shards[shard] != kv.gid) && 
			(kv.CurShardStatus[shard] != NOTOWNED) {
			return false
		}
	}

	return true
}

func (kv *ShardKV) OnApplyConfigNolock(op Op) {

	masterConfig := op.ApplyConfig
	if masterConfig.Num < kv.MasterShardConfig.Num {
		return
	}
	if masterConfig.Num > kv.MasterShardConfig.Num {
		kv.MasterShardConfig = masterConfig
	}

	if kv.AlreadyStable() {
		return
	}

	if kv.AppliedShardConfig.Num == 0 {
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.MasterShardConfig.Shards[shard] == kv.gid {
				kv.CurShardStatus[shard] = AVALIABLE
			} else {
				kv.CurShardStatus[shard] = NOTOWNED
			}
		}
	
		kv.UpdateAppliedShardConfigIfNeededNolock(masterConfig.Num)
		return
	}

	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.CurShardStatus[shard] == FINISH_IMPORTING ||
			kv.CurShardStatus[shard] == FINISH_EXPORTING ||
			kv.CurShardStatus[shard] == BUSY_EXPORTING {
			continue
		}

		if kv.AppliedShardConfig.Shards[shard] == kv.gid &&
			kv.MasterShardConfig.Shards[shard] != kv.gid {
			kv.CurShardStatus[shard] = EXPORTING
		} else if kv.AppliedShardConfig.Shards[shard] != kv.gid &&
			kv.MasterShardConfig.Shards[shard] == kv.gid {
			kv.CurShardStatus[shard] = IMPORTING
		} else if kv.AppliedShardConfig.Shards[shard] == kv.gid {
			kv.CurShardStatus[shard] = AVALIABLE
		} else {
			kv.CurShardStatus[shard] = NOTOWNED
		}
	}
	
	kv.UpdateAppliedShardConfigIfNeededNolock(masterConfig.Num)


	for shard := 0; shard < shardmaster.NShards; shard++ {

		if kv.CurShardStatus[shard] == EXPORTING {
			isLeader := kv.rf.IsLeaderNolock()
			if isLeader {
				kv.ExportingShardNolock(shard, kv.MasterShardConfig.Shards[shard])
			}

		} 
	}

}

func (kv *ShardKV) OnApplyEntry(m *raft.ApplyMsg) {
	ops := m.Command.(Op)

	//fmt.Printf("gid(%d) me(%d), leader(%t) OnApplyEntry.1,%+v\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(), ops)
	
	kv.mu.Lock()
	//fmt.Printf("gid(%d) me(%d), leader(%t) OnApplyEntry.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	dup, ok := kv.GetDuplicateNolock(ops.ClientId, ops.Key)
	shardNum := key2shard(ops.Key)
	defer kv.mu.Unlock()
	//defer fmt.Printf("gid(%d) me(%d), leader(%t) OnApplyEntry.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	

	if !ok || (dup != ops.RequestId) {
		// save the client id and its serial number
		switch ops.Method {
		case "Get":
			// nothing
		case "Put":
			kv.Kvmap[shardNum][ops.Key] = ops.Value
			kv.SetDuplicateNolock(ops.ClientId, ops.Key, ops.RequestId)
		case "Append":
			kv.Kvmap[shardNum][ops.Key] += ops.Value
			kv.SetDuplicateNolock(ops.ClientId, ops.Key, ops.RequestId)
		case "ApplyConfig":
			kv.OnApplyConfigNolock(ops)
		case "ImportComplete":
			kv.OnImportCompleteNolock(ops)
		case "ExportComplete":
			kv.OnExportCompleteNolock(ops)
		default:
			fmt.Printf("OnApplyEntry, unknown ops.Method:%v", ops.Method)
		}
	}


	ch, ok := kv.RequestHandlers[m.CommandIndex]
	if ok {

		delete(kv.RequestHandlers, m.CommandIndex)

		ch <- *m

	}

}

func (kv *ShardKV) RaftBecomeFollower(m *raft.ApplyMsg) bool {
	return (m.CommandValid == false) && 
		(m.Type == raft.MsgTypeRole) &&
		(m.Role == raft.RoleFollower)
}

func (kv *ShardKV) OnRoleNotify(m *raft.ApplyMsg) {

		 //fmt.Printf("gid(%d) me(%d), leader(%t) OnRoleNotify.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
		 //fmt.Printf("gid(%d) me(%d), leader(%t) OnRoleNotify.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	defer kv.mu.Unlock()

	if kv.RaftBecomeFollower(m) {
	    for index, ch := range kv.RequestHandlers {
			delete(kv.RequestHandlers, index)
			ch <- *m
	    }	
	}
		 //fmt.Printf("gid(%d) me(%d), leader(%t) OnRoleNotify.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	

}

func (kv *ShardKV) OnSnapshot(m *raft.ApplyMsg) {
	//fmt.Printf("gid(%d) me(%d), leader(%t) OnSnapshot.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
	//fmt.Printf("gid(%d) me(%d), leader(%t) OnSnapshot.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	defer kv.mu.Unlock()

	kv.loadSnapshot(m.Snapshot)
	//fmt.Printf("gid(%d) me(%d), leader(%t) OnSnapshot.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
}

func (kv *ShardKV) DoSnapshot(lastIncludedIndex int) {
		//fmt.Printf("gid(%d) me(%d), leader(%t) DoSnapshot.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	kv.mu.Lock()
		//fmt.Printf("gid(%d) me(%d), leader(%t) DoSnapshot.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	
	defer kv.mu.Unlock()
	//defer 	//fmt.Printf("gid(%d) me(%d), leader(%t) DoSnapshot.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	

	if lastIncludedIndex <= kv.lastSnapshotIndex {
		return
	}
	kv.lastSnapshotIndex = lastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Kvmap)
	e.Encode(kv.lastSnapshotIndex)
	e.Encode(kv.Duplicate)
	e.Encode(kv.CurShardStatus)
	e.Encode(kv.AppliedShardConfig)
	e.Encode(kv.MasterShardConfig)
	snapshot := w.Bytes()
	//fmt.Printf("Snapshot size=%d\n", len(snapshot))

	kv.snapshoting = true
	//fmt.Printf("gid(%d) me(%d), leader(%t) DoSnapshot.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
	go func() {

		kv.rf.PersistStateAndSnapshot(kv.lastSnapshotIndex, snapshot)
		kv.snapshoting = false
	}()
}

func (kv *ShardKV) SnapshotIfNeeded(index int) {
	if kv.maxraftstate > 0 && !kv.snapshoting {
		var threshold = int(0.9 * float64(kv.maxraftstate))
		if kv.rf.GetRaftStateSize() > threshold {
			kv.DoSnapshot(index)
		}
	}

}

func (kv *ShardKV) receivingApplyMsg() {
	for {
		select {
		case m := <-kv.applyCh:
				if m.CommandValid {
					kv.OnApplyEntry(&m)
					kv.SnapshotIfNeeded(m.CommandIndex)
				} else if(m.Type == raft.MsgTypeKill) {
					close(kv.shutdown) 
					return 
				} else if(m.Type == raft.MsgTypeRole) {
					kv.OnRoleNotify(&m)
				} else if(m.Type == raft.MsgTypeSnapshot) {
					kv.OnSnapshot(&m)
				}
		}

	}
}

func (kv *ShardKV) refreshShardMasterConfig() {
	for {
		select {
		case <- time.After(ShardMasterCheckInterval):
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				continue
			}

			masterShardConfig := kv.sm.Query(-1)

			//fmt.Printf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			kv.mu.Lock()
			//fmt.Printf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			

			if masterShardConfig.Num > kv.AppliedShardConfig.Num+1 {
				//fmt.Printf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.2.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
				kv.MasterShardConfig = kv.sm.Query(kv.AppliedShardConfig.Num+1)
				//fmt.Printf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.2.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())

			} else {

				kv.MasterShardConfig = masterShardConfig
			}


			var copyMasterShardConfig shardmaster.Config
			copyMasterShardConfig.Groups = map[int][]string{}
			CloneConfig(&kv.MasterShardConfig, &copyMasterShardConfig)

			appliedConfigNum := kv.AppliedShardConfig.Num


			//fmt.Printf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			
			kv.mu.Unlock()

			if copyMasterShardConfig.Num > appliedConfigNum {

				ops := Op {
					Method: "ApplyConfig",
					ApplyConfig: copyMasterShardConfig,
				}

				kv.rf.Start(ops)

			}

		case <- kv.shutdown:
			return		
		}

	}	
}


func (kv *ShardKV) loadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	
	snapshotIndex := 0
	var kvmap [shardmaster.NShards]map[string]string
	var duplicate [shardmaster.NShards]map[int64]map[string]int64
	var shardStatus [shardmaster.NShards]ShardStatus
	var shardConfig shardmaster.Config
	var masterShardConfig shardmaster.Config

	for i:=0;i<shardmaster.NShards; i++ {
		kvmap[i] = make(map[string]string)
		duplicate[i] = make(map[int64]map[string]int64)
	}

	d.Decode(&kvmap)
	d.Decode(&snapshotIndex)
	d.Decode(&duplicate)
	d.Decode(&shardStatus)
	d.Decode(&shardConfig)
	d.Decode(&masterShardConfig)

	
	kv.lastSnapshotIndex = snapshotIndex
	kv.Kvmap = kvmap
	kv.Duplicate = duplicate
	kv.CurShardStatus = shardStatus
	kv.AppliedShardConfig = shardConfig
	kv.MasterShardConfig = masterShardConfig
	
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
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
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.shutdown = make(chan struct{})
	kv.RequestHandlers = make(map[int]chan raft.ApplyMsg)
	for i:=0;i<shardmaster.NShards; i++ {
		kv.Kvmap[i] = make(map[string]string)
		kv.Duplicate[i] = make(map[int64]map[string]int64)
	}
	
	if kv.maxraftstate > 0 {
		data := persister.ReadSnapshot()
		if len(data) > 0 {
			kv.loadSnapshot(data)
		}
	}
	// //fmt.Printf("kvshard server %d start, value=%+v\n\n", kv.me, kv)
	// You may need initialization code here.

	go kv.receivingApplyMsg()
	go kv.refreshShardMasterConfig()

	return kv
}
