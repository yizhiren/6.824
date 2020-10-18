package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	duplicate map[int64]map[string]int64 // [clientId]([key]requestId)
	requestHandlers map[int]chan raft.ApplyMsg

	configs []Config // indexed by config num
	commitIndex int
}

func (sm *ShardMaster) Lock() {
	sm.mu.Lock()
}

func (sm *ShardMaster) Unlock() {
	sm.mu.Unlock()
}

func (sm *ShardMaster) registerIndexHandler(index int) chan raft.ApplyMsg {
	sm.Lock()
	defer sm.Unlock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	sm.requestHandlers[index] = awaitChan

	return awaitChan
}


func (sm *ShardMaster) GetDuplicate(clientId int64, method string) (int64, bool) {
	sm.Lock()
	defer sm.Unlock()

	clientRequest, haveClient := sm.duplicate[clientId]
	if !haveClient {
		return 0,false
	}
	val, ok := clientRequest[method]
	return val, ok
}

func (sm *ShardMaster) SetDuplicateNolock(clientId int64, method string, requestId int64)  {
	_, haveClient := sm.duplicate[clientId]
	if !haveClient {
		sm.duplicate[clientId] = make(map[string]int64)
	}
	sm.duplicate[clientId][method] = requestId
}



type Op struct {
	// Your data here.
	Method string
	Config Config
	RequestId int64
	ClientId int64
}

/*
func Clone(a, b interface{}) {
    buff := new(bytes.Buffer)
    enc := labgob.NewEncoder(buff)
    dec := labgob.NewDecoder(buff)
    enc.Encode(a)
    dec.Decode(b)

    DPrintf("Clone, a=%+v,b=%+v\n", a, b)
}
*/

func Clone(a, b *Config)  {
	b.Num = a.Num
	for i,gid := range a.Shards {
		b.Shards[i] = gid
	}
	for gid := range a.Groups {
		// 假定了一个group中的机器不变
		b.Groups[gid] = a.Groups[gid]
	}
}


func (sm *ShardMaster) AppendConfigAfterJoin(args *JoinArgs) Config {
	sm.Lock()
	defer sm.Unlock()

	newConfig := Config{}
	newConfig.Groups = map[int][]string{}
	lastConfig := sm.configs[len(sm.configs) - 1]
	Clone(&lastConfig, &newConfig)
	newConfig.Num = len(sm.configs)
	for gid, names := range args.Servers {
		newConfig.Groups[gid] = names
	}
	DPrintf("NewConfigAfterJoin, lastConfig=%+v, newConfig=%+v, args=%+v",
		lastConfig, newConfig, args)

	sm.RebalanceNolock(&newConfig)
	sm.configs = append(sm.configs, newConfig)
	return newConfig
}

func (sm *ShardMaster) AppendConfigAfterLeave(args *LeaveArgs) Config {
	sm.Lock()
	defer sm.Unlock()

	newConfig := Config{}
	newConfig.Groups = map[int][]string{}
	lastConfig := sm.configs[len(sm.configs) - 1]
	Clone(&lastConfig, &newConfig)
	newConfig.Num = len(sm.configs)
	for _,gid := range args.GIDs {
		delete(newConfig.Groups,gid)
	}
	DPrintf("NewConfigAfterLeave, lastConfig=%+v, newConfig=%+v, args=%+v",
		lastConfig, newConfig, args)

	sm.RebalanceNolock(&newConfig)
	sm.configs = append(sm.configs, newConfig)
	return newConfig
}

func (sm *ShardMaster) AppendConfigAfterMove(args *MoveArgs) Config {
	sm.Lock()
	defer sm.Unlock()

	newConfig := Config{}
	newConfig.Groups = map[int][]string{}
	lastConfig := sm.configs[len(sm.configs) - 1]
	Clone(&lastConfig, &newConfig)
	newConfig.Num = len(sm.configs)
	newConfig.Shards[args.Shard] = args.GID

	DPrintf("NewConfigAfterMove, lastConfig=%+v, newConfig=%+v, args=%+v",
		lastConfig, newConfig, args)

	sm.configs = append(sm.configs, newConfig)
	return newConfig
}

func (sm *ShardMaster) RebalanceNolock(config *Config) {

	// balance shards to latest groups	
	numOfGroup := len(config.Groups)
	if numOfGroup > 0 {
		//numOfNodesPerGroup := NShards / numOfGroup
		//log.Println("num of shards per group is", numOfNodesPerGroup)

		leftOver := NShards % numOfGroup

		
		for i:=0; i< NShards - leftOver; {
			for gid := range config.Groups {
				//log.Println("shard is", i, "group id is", gid)
				config.Shards[i] = gid
				i++
			}
		}

		groupList := make([]int, 0)
		for gid := range config.Groups {
			groupList = append(groupList, gid)
		}

		// add left over shards
		for j:=NShards-leftOver; j<NShards && len(groupList) > 0; j++ {
			nextGroup := (j % numOfGroup)
			config.Shards[j] = groupList[nextGroup]
		} 

		DPrintf("RebalanceNolock result %+v\n", config.Shards)
	}

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("Join request: args:%v\n", args)
	defer DPrintf("Join response: reply:%v\n", reply)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	methodName := "Join"
	newConfig := sm.AppendConfigAfterJoin(args)

	ops := Op {
		Method: methodName,
		Config: newConfig,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

	dup, ok := sm.GetDuplicate(ops.ClientId, ops.Method)
	if ok && (dup == args.RequestId) {
		reply.Err = OK
		return
	}

	index, term, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sm.await(index, term, ops)
	if !success {
		reply.WrongLeader = true
		return
	} else {	
		reply.Err = OK
		DPrintf("Join Success: args:%v\n", args)
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("Leave request: args:%v\n", args)
	defer DPrintf("Leave response: reply:%v\n", reply)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	methodName := "Leave"
	newConfig := sm.AppendConfigAfterLeave(args)

	ops := Op {
		Method: methodName,
		Config: newConfig,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

	dup, ok := sm.GetDuplicate(ops.ClientId, ops.Method)
	if ok && (dup == args.RequestId) {
		reply.Err = OK
		return
	}

	index, term, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sm.await(index, term, ops)
	if !success {
		reply.WrongLeader = true
		return
	} else {	
		reply.Err = OK
		DPrintf("Leave Success: args:%v\n", args)
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("Move request: args:%v\n", args)
	defer DPrintf("Move response: reply:%v\n", reply)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	methodName := "Move"
	newConfig := sm.AppendConfigAfterMove(args)

	ops := Op {
		Method: methodName,
		Config: newConfig,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

	dup, ok := sm.GetDuplicate(ops.ClientId, ops.Method)
	if ok && (dup == args.RequestId) {
		reply.Err = OK
		return
	}

	index, term, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sm.await(index, term, ops)
	if !success {
		reply.WrongLeader = true
		return
	} else {	
		reply.Err = OK
		DPrintf("Move Success: args:%v\n", args)
		return
	}
}

func (sm *ShardMaster) getConfig(index int) Config {
	sm.Lock()
	defer sm.Unlock()

	var config Config
	config.Groups = map[int][]string{}
	if (index < 0) || (index >sm.commitIndex) {
		Clone(&sm.configs[sm.commitIndex], &config) 
	} else {
		Clone(&sm.configs[index], &config)
	}

	return config
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("Query request: args:%v\n", args)
	defer DPrintf("Query response: reply:%v\n", reply)

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	methodName := "Query"
	theCareConfig := sm.getConfig(args.Num)

	ops := Op {
		Method: methodName,
		Config: theCareConfig,
		RequestId: args.RequestId,
		ClientId: args.ClientId,
	}

/*
	dup, ok := sm.GetDuplicate(ops.ClientId, ops.Method)
	if ok && (dup == args.RequestId) {
		reply.Err = OK
		return
	}
*/

	index, term, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sm.await(index, term, ops)
	if !success {
		reply.WrongLeader = true
		return
	} else {
		reply.Config = theCareConfig
		reply.Err = OK
		return
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) await(index int, term int, op Op) (success bool) {
	awaitChan := sm.registerIndexHandler(index)

	for {
		select {
		case message := <-awaitChan:
			if sm.RaftBecomeFollower(&message) {
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

func (sm *ShardMaster) OnApplyEntry(m *raft.ApplyMsg) {
	ops := m.Command.(Op)
	dup, ok := sm.GetDuplicate(ops.ClientId, ops.Method)
	sm.Lock()
	defer sm.Unlock()

	if !ok || (dup != ops.RequestId) {
		switch ops.Method {
		case "Leave":
			if ops.Config.Num >= len(sm.configs) {
				sm.configs = append(sm.configs,ops.Config)
				sm.SetDuplicateNolock(ops.ClientId, ops.Method, ops.RequestId)
			}
			sm.commitIndex = ops.Config.Num
		case "Join":
			if ops.Config.Num >= len(sm.configs) {
				sm.configs = append(sm.configs,ops.Config)
				sm.SetDuplicateNolock(ops.ClientId, ops.Method, ops.RequestId)
			}
			sm.commitIndex = ops.Config.Num
		case "Move":
			if ops.Config.Num >= len(sm.configs) {
				sm.configs = append(sm.configs,ops.Config)
				sm.SetDuplicateNolock(ops.ClientId, ops.Method, ops.RequestId)
			}
			sm.commitIndex = ops.Config.Num
		}
	}

	ch, ok := sm.requestHandlers[m.CommandIndex]
	if ok {
		delete(sm.requestHandlers, m.CommandIndex)
		ch <- *m
	}
}

func (sm *ShardMaster) RaftBecomeFollower(m *raft.ApplyMsg) bool {
	return (m.CommandValid == false) && 
		(m.Type == raft.MsgTypeRole) &&
		(m.Role == raft.RoleFollower)
}

func (sm *ShardMaster) OnRoleNotify(m *raft.ApplyMsg) {
	sm.Lock()
	defer sm.Unlock()

	if sm.RaftBecomeFollower(m) {
	    for index, ch := range sm.requestHandlers {
			delete(sm.requestHandlers, index)
			ch <- *m
	    }	
	}

}

func (sm *ShardMaster) receivingApplyMsg() {
	for {
		select {
		case m := <-sm.applyCh:
				if m.CommandValid {
					DPrintf("receivingApplyMsg receive entry message. %+v.", m)
					sm.OnApplyEntry(&m)
					DPrintf("new configs after apply. %+v.", sm.configs)
				} else if(m.Type == raft.MsgTypeKill) {
					DPrintf("receivingApplyMsg receive kill message. %+v.", m)
					return 
				} else if(m.Type == raft.MsgTypeRole) {
					//DPrintf("receivingApplyMsg receive role message. %+v.", m)
					sm.OnRoleNotify(&m)
					
				} 
		}

	}
}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.duplicate = make(map[int64]map[string]int64)
	sm.requestHandlers = make(map[int]chan raft.ApplyMsg)
	sm.commitIndex = 0

	go sm.receivingApplyMsg()

	return sm
}
