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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const ShardMasterCheckInterval = 50 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key string
	Value string
	RequestId int64
	ClientId int64

	// METHOD_APPLY_CONFIG
	ApplyConfig shardmaster.Config
	// METHOD_IMPORT_COMPLETE  METHOD_EXPORT_COMPLETE
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

)

const (
	METHOD_GET = "Get"
	METHOD_PUT = "Put"
	METHOD_APPEND = "Append"
	METHOD_APPLY_CONFIG = "ApplyConfig"
	METHOD_IMPORT_COMPLETE = "ImportComplete"
	METHOD_EXPORT_COMPLETE = "ExportComplete"
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
	isStartupFinish bool
}

func (kv *ShardKV) registerIndexHandler(index int) chan raft.ApplyMsg {

	kv.mu.Lock()

	defer kv.mu.Unlock()

	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.RequestHandlers[index] = awaitChan

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

				if op.Key == "" {
					return (term == message.CommandTerm)
				} else {
					return (term == message.CommandTerm) &&
						kv.IsOwnThisKeyNolock(op.Key) 
						// is key still here?, not transfered
						// thie code is under the lock of onApplyEntry
				}
			}
			// continue
		}
	}
}

func (kv *ShardKV) GetKeyValue(key string) (string, bool) {

	kv.mu.Lock()

	defer kv.mu.Unlock()
	
	shardNum := key2shard(key)
	val, ok := kv.Kvmap[shardNum][key]


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

	kv.mu.Lock()

	if !kv.IsOwnThisKeyNolock(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}


	ops := Op {
		Method: METHOD_GET,
		Key: args.Key,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}


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


	kv.mu.Lock()

	if !kv.IsOwnThisKeyNolock(args.Key) {

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

		kv.mu.Unlock()
		reply.Err = OK
		return
	}

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

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	kv.mu.Lock()

	// 这边还没准备好，请求中的版本太新了
	if kv.AppliedShardConfig.Num + 1 < args.DstConfigVersion {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 已经达成了，说明前面这个数据已经迁过来了
	if kv.AppliedShardConfig.Num >= args.DstConfigVersion {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	// 已经更新好了
	if (kv.CurShardStatus[args.ShardNumber] == AVALIABLE) ||
		 (kv.CurShardStatus[args.ShardNumber] == FINISH_IMPORTING) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	// 匹配不上，还没准备好，需要等会重试
	if kv.CurShardStatus[args.ShardNumber] != IMPORTING {
		reply.Err = ErrRetry
		kv.mu.Unlock()
		return
	}

	ops := Op {
		Method : METHOD_IMPORT_COMPLETE,
		MigrateShard: *args,
	}

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
	DPrintf("gid(%d) me(%d), leader(%t) ExportingShardNolock sending args=%+v\n", kv.gid, kv.me, kv.rf.IsLeaderNolock(), args)
	go func(){
		for si := 0; si < len(destServers); si++ {
			srv := kv.make_end(destServers[si])
			var reply MigrateShardReply
	
			DPrintf("gid(%d) me(%d), leader(%t) ExportingShardNolock R1 %v\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(),destServers[si])
			ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
			DPrintf("gid(%d) me(%d), leader(%t) ExportingShardNolock R2 %v\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(),destServers[si])
			
			if ok && reply.Err == OK {

				thinArgs := MigrateShardArgs{
					SrcGid: args.SrcGid,
					DstGid: args.DstGid,
					DstConfigVersion: args.DstConfigVersion,
					ShardNumber: args.ShardNumber,
					Kvmap : nil,
					Duplicate : nil,
				}

				ops := Op {
					Method : METHOD_EXPORT_COMPLETE,
					MigrateShard: thinArgs,
				}
				DPrintf("gid(%d) me(%d), leader(%t) ExportingShardNolock success[+], ops=%+v\n", kv.gid, kv.me, kv.rf.IsLeaderNolock(), ops)
				kv.rf.Start(ops)
				return
			}

			
			if ok && reply.Err == ErrRetry {
				break
			}

			if ok && reply.Err == ErrWrongGroup {
				break
			}

		}

	}()
	

}

func (kv *ShardKV) IsNotFinishBalance() bool {
	for _, status := range kv.CurShardStatus {
		if status == IMPORTING || 
			status == EXPORTING  {
			return true
		}
	}
	return false
}

func (kv *ShardKV) UpdateAppliedShardConfigIfNeededNolock() {

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

	DPrintf("gid(%d) me(%d), leader(%t) Current Status, %+v, upgrade to version %+v\n",
		kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.CurShardStatus, kv.AppliedShardConfig)
}

func (kv *ShardKV) OnImportCompleteNolock(op Op) {

	args := &op.MigrateShard

	if (kv.MasterShardConfig.Num == args.DstConfigVersion) &&
		(kv.CurShardStatus[args.ShardNumber] == IMPORTING) {

		// 当前持久化库只存引用没有序列化，所以需要拷贝一份避免entry log数据被修改
		kvmap := map[string]string{}
		duplicate := map[int64]map[string]int64{}
		CloneKv(&args.Kvmap, &kvmap)
		CloneDuplicate(&args.Duplicate, &duplicate)
		kv.Kvmap[args.ShardNumber] = kvmap
		kv.Duplicate[args.ShardNumber] = duplicate

		kv.CurShardStatus[args.ShardNumber] = FINISH_IMPORTING
		kv.UpdateAppliedShardConfigIfNeededNolock()

	} else {
		//DPrintf("OnImportComplete fail, kv.CurShardStatus[%d]=%d, args=%+v",
		//	args.ShardNumber, kv.CurShardStatus[args.ShardNumber], args)
	}

}

func (kv *ShardKV) OnExportCompleteNolock(op Op) {

	args := &op.MigrateShard

	if (kv.MasterShardConfig.Num == args.DstConfigVersion) &&
		(kv.CurShardStatus[args.ShardNumber] == EXPORTING) {
		kv.Kvmap[args.ShardNumber] = make(map[string]string)
		kv.Duplicate[args.ShardNumber] = make(map[int64]map[string]int64)
		kv.CurShardStatus[args.ShardNumber] = FINISH_EXPORTING
		kv.UpdateAppliedShardConfigIfNeededNolock()
	} else {
		//DPrintf("OnExportComplete fail, kv.CurShardStatus[%d]=%d, args=%+v",
		//	args.ShardNumber, kv.CurShardStatus[args.ShardNumber], args)
	}

}

/*
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
*/

func (kv *ShardKV) OnApplyConfigNolock(op Op) {
	//DPrintf("OnApplyConfigNolock applied config=%+v, masterConfig=%+v, op config=%+v\n",
	//		kv.AppliedShardConfig, kv.MasterShardConfig, op)
	if op.ApplyConfig.Num == kv.AppliedShardConfig.Num + 1 {

		if kv.IsNotFinishBalance() {
			return 
		}

		kv.MasterShardConfig = op.ApplyConfig

		if kv.AppliedShardConfig.Num == 0 {
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.MasterShardConfig.Shards[shard] == kv.gid {
					kv.CurShardStatus[shard] = AVALIABLE
				} else {
					kv.CurShardStatus[shard] = NOTOWNED
				}
			}
		

			kv.AppliedShardConfig = kv.MasterShardConfig
			DPrintf("gid(%d) me(%d), leader(%t) upgrade to version 1:%+v\n", 
				kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.AppliedShardConfig)
			return
		}

		for shard := 0; shard < shardmaster.NShards; shard++ {

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
		kv.UpdateAppliedShardConfigIfNeededNolock()
		return

	} else {
		DPrintf("OnApplyConfigNolock got a wrong config, op=%+v, kv.MasterShardConfig=%+v\n",
			op, kv.MasterShardConfig)
	}
	
}

func (kv *ShardKV) RebalanceShard() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.MasterShardConfig.Num != kv.AppliedShardConfig.Num +1 {
		DPrintf("gid(%d) me(%d), leader(%t), Config Version Wrong (%d) != (%d+1)\n",
			kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.MasterShardConfig.Num, kv.AppliedShardConfig.Num)
		return
	}

	DPrintf("gid(%d) me(%d), leader(%t), cur shard status (%+v).\n",
			kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.CurShardStatus)
		
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.CurShardStatus[shard] == EXPORTING {
			kv.ExportingShardNolock(shard, kv.MasterShardConfig.Shards[shard])
		} 
	}

}

func (kv *ShardKV) OnApplyEntry(m *raft.ApplyMsg) {
	ops := m.Command.(Op)


	kv.mu.Lock()
	defer kv.mu.Unlock()

	dup, ok := kv.GetDuplicateNolock(ops.ClientId, ops.Key)
	shardNum := key2shard(ops.Key)
	

	if !ok || (dup != ops.RequestId) {
		// save the client id and its serial number
		switch ops.Method {
		case METHOD_GET:
			// nothing
		case METHOD_PUT:
			kv.Kvmap[shardNum][ops.Key] = ops.Value
			kv.SetDuplicateNolock(ops.ClientId, ops.Key, ops.RequestId)
		case METHOD_APPEND:
			kv.Kvmap[shardNum][ops.Key] += ops.Value
			kv.SetDuplicateNolock(ops.ClientId, ops.Key, ops.RequestId)
		case METHOD_APPLY_CONFIG:
			kv.OnApplyConfigNolock(ops)
		case METHOD_IMPORT_COMPLETE:
			kv.OnImportCompleteNolock(ops)
		case METHOD_EXPORT_COMPLETE:
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


	kv.mu.Lock()

	defer kv.mu.Unlock()

	if kv.RaftBecomeFollower(m) {
	    for index, ch := range kv.RequestHandlers {
			delete(kv.RequestHandlers, index)
			ch <- *m
	    }	
	}

}

func (kv *ShardKV) OnSnapshot(m *raft.ApplyMsg) {

	kv.mu.Lock()

	defer kv.mu.Unlock()

	kv.loadSnapshot(m.Snapshot)

}

func (kv *ShardKV) DoSnapshot(lastIncludedIndex int) {

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
	e.Encode(kv.CurShardStatus)
	e.Encode(kv.AppliedShardConfig)
	e.Encode(kv.MasterShardConfig)
	snapshot := w.Bytes()

	kv.snapshoting = true
	
	go func() {
		kv.rf.PersistStateAndSnapshot(kv.lastSnapshotIndex, snapshot)
		kv.snapshoting = false
	}()
}

func (kv *ShardKV) SnapshotIfNeeded(index int) {
	if kv.maxraftstate > 0 && !kv.snapshoting {
		var threshold = int(0.95 * float64(kv.maxraftstate))
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
				} else if(m.Type == raft.MsgTypeStartup) {
					kv.isStartupFinish = true
				}
		}

	}
}

func (kv *ShardKV) fetchNextConfig() {
		DPrintf("gid(%d) me(%d), leader(%t) fetchNextConfig.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			
		masterShardConfig := kv.sm.Query(-1)
		DPrintf("gid(%d) me(%d), leader(%t) fetchNextConfig.2 sm-master(%+v), this-master(%+v), this-applied(%+v)\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(),
			masterShardConfig, kv.MasterShardConfig, kv.AppliedShardConfig)
			
		kv.mu.Lock()
		DPrintf("gid(%d) me(%d), leader(%t) fetchNextConfig.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			
		if masterShardConfig.Num == kv.AppliedShardConfig.Num + 1 {
			//
		} else if masterShardConfig.Num > kv.AppliedShardConfig.Num + 1 {
			DPrintf("gid(%d) me(%d), leader(%t) fetchNextConfig.3.1, AppliedShardConfig=%d\n",kv.gid, kv.me, kv.rf.IsLeaderNolock(), kv.AppliedShardConfig.Num)
			
			masterShardConfig = kv.sm.Query(kv.AppliedShardConfig.Num + 1)
			DPrintf("gid(%d) me(%d), leader(%t) fetchNextConfig.3.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			
		} else {
			DPrintf("gid(%d) me(%d), leader(%t) fetchNextConfig.4\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		
		if masterShardConfig.Num != kv.AppliedShardConfig.Num + 1 {
			DPrintf("gid(%d) me(%d), leader(%t) invalid masterShardConfig", kv.gid, kv.me, kv.rf.IsLeaderNolock())
			return
		}

		ops := Op {
			Method: "ApplyConfig",
			ApplyConfig: masterShardConfig,
		}

		kv.rf.Start(ops)
		
}

func (kv *ShardKV) refreshShardMasterConfig() {
	for {
		select {
		case <- time.After(ShardMasterCheckInterval):
			DPrintf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.1\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			if !kv.isStartupFinish {
				continue
			}

			_, isLeader := kv.rf.GetState()
			if !isLeader {
				continue
			}
			DPrintf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.2\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			if (kv.MasterShardConfig.Num > kv.AppliedShardConfig.Num) {
				kv.RebalanceShard()
				continue
			}
			DPrintf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.3\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
			kv.fetchNextConfig()
			DPrintf("gid(%d) me(%d), leader(%t) refreshShardMasterConfig.4\n",kv.gid, kv.me, kv.rf.IsLeaderNolock())
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
	kv.sm.Kill()
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.receivingApplyMsg()
	go kv.refreshShardMasterConfig()

	return kv
}
