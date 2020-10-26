package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// import "sort"
import (
	"bytes"
	"sync"
	"sync/atomic"
	"../labrpc"
	"../labgob"
	"math/rand"
	"time"
	"fmt"
)

// import "bytes"
// import "../labgob"

type Entry struct {
	Index       int
	Term        int
	Command     interface{}
}

const (
	RoleFollower    = 1
	RoleCandidate   = 2
	RoleLeader      = 3
)

const (
	MsgTypeEntry = 0
	MsgTypeRole = 1
	MsgTypeKill = 2
	MsgTypeSnapshot = 3
	MsgTypeStartup = 4
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
	//
	Type int
	Role int
	Snapshot []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//
	currentTerm int
	votedFor int
	log []Entry
	totalVotes int
	//
	commitIndex int
	lastApplied int
	startupCommitIndex int //启动时候的commitindex，用于判断启动完成
	//
	nextIndex []int
	matchIndex []int
	//
	lastSnapshotIndex int
	lastSnapshotTerm int
	//
	role int
	applyCh chan ApplyMsg
	timeoutToCandidate int
	timeoutToReCandidate int
	timeoutToHeartbeat int
	timeoutToApplyLog int
	cancelToCandidate bool
	cancelToReCandidate bool

	snapshotToNotify []*InstallSnapshotArgs
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func GenerateElectionTimeout(min, max int) int {
	rad := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rad.Intn(max - min) + min
	return randNum
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//
	term = rf.currentTerm
	isleader = (rf.role == RoleLeader)

	//
	return term, isleader
}

func (rf *Raft) IsLeaderNolock() bool {
	return (rf.role == RoleLeader)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 data := rf.getPersistState()
	 rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	 r := bytes.NewBuffer(data)
	 d := labgob.NewDecoder(r)
	 var currentTerm int
	 var votedFor int
	 var log []Entry
	 var lastSnapshotIndex int
	 var lastSnapshotTerm int
	 var commitIndex int
	 if d.Decode(&currentTerm) != nil ||
	    d.Decode(&votedFor) != nil ||
	    d.Decode(&log) != nil ||
	    d.Decode(&lastSnapshotIndex) != nil ||
	    d.Decode(&lastSnapshotTerm) != nil ||
	    d.Decode(&commitIndex) != nil {

	 } else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
	   rf.log = log
	   rf.lastSnapshotIndex = lastSnapshotIndex
	   rf.lastSnapshotTerm = lastSnapshotTerm
	   rf.lastApplied = lastSnapshotIndex
	   rf.commitIndex = commitIndex
	 }
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
	Success bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

// fast append entry protocol
/*
	If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
	If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
	Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
	If it does not find an entry with that term, it should set nextIndex = conflictIndex.
*/

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) getPersistState() []byte {
	 w := new(bytes.Buffer)
	 e := labgob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.log)
	 e.Encode(rf.lastSnapshotIndex)
	 e.Encode(rf.lastSnapshotTerm)
	 e.Encode(rf.commitIndex)
	 data := w.Bytes()
	 return data
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) PersistStateAndSnapshot(lastIncludedIndex int, snapshot [] byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex > rf.lastSnapshotIndex {
		offsetIndex := lastIncludedIndex-rf.lastSnapshotIndex
		if rf.log[offsetIndex].Index != lastIncludedIndex {
			fmt.Printf("some thing is wrong. index not match, lastIncludedIndex=%d, lastSnapshotIndex=%d, offsetIndex=%d, log index=%d.", 
				lastIncludedIndex, rf.lastSnapshotIndex, offsetIndex, rf.log[offsetIndex].Index)
			return
		}

		entry := rf.log[offsetIndex]
		rf.lastSnapshotIndex = entry.Index
		rf.lastSnapshotTerm = entry.Term
		rf.log = rf.log[offsetIndex:]

		data := rf.getPersistState()
		DPrintf("Server %d: start PersistStateAndSnapshot, last entry=%+v\n", rf.me, entry)
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("RequestVote called, arg=%+v,rf=%+v\n", args, rf)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: got RequestVote from server %d, args: %+v, current term: %d, current commitIndex: %d, current log: %v\n", 
		rf.me, args.CandidateId, args, rf.currentTerm, rf.commitIndex, rf.log)
	
	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Server %d: got RequestVote from server %d, return false 1.\n", 
			rf.me, args.CandidateId)
		return
	}

	if args.Term == rf.currentTerm &&
		rf.votedFor != -1 && 
		rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Server %d: got RequestVote from server %d, return false 2.\n", 
			rf.me, args.CandidateId)
		return		
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, -1)
	}

	lastLogOffsetIndex := len(rf.log) - 1
	lastLogIndex := lastLogOffsetIndex + rf.lastSnapshotIndex
	if (args.LastLogTerm < rf.log[lastLogOffsetIndex].Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Server %d: got RequestVote from server %d, return false 3.\n", 
			rf.me, args.CandidateId)
		return	
	}

	if (args.LastLogTerm == rf.log[lastLogOffsetIndex].Term) &&
	   (args.LastLogIndex < lastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Server %d: got RequestVote from server %d, return false 4.\n", 
			rf.me, args.CandidateId)
		return
	}

	rf.cancelToCandidate = true
	rf.cancelToReCandidate = true

	DPrintf("Server %d: got RequestVote from server %d, return true.\n", 
		rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func min(x, y int) int {
    if x < y {
        return x
    } else {
        return y
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("AppendEntries called, [+]rf=%+v\n", rf)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("AppendEntries called, [-]arg=%+v\n", args)
	DPrintf("Server %d: got AppendEntries from leader %d, args: %+v, current term: %d, current commitIndex: %d, current log: %v\n", 
		rf.me, args.LeaderId, args, rf.currentTerm, rf.commitIndex, rf.log)
	
	if args.Term < rf.currentTerm {
		DPrintf("Server %d: got AppendEntries from leader %d, return false 1.\n", 
			rf.me, args.LeaderId)

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} 

	rf.cancelToCandidate = true
	rf.cancelToReCandidate = true
	rf.convertToFollower(args.Term, args.LeaderId)
	
	preLogOffsetIndex := args.PrevLogIndex - rf.lastSnapshotIndex
	if preLogOffsetIndex > 0 {
		if preLogOffsetIndex >= len(rf.log) {
			DPrintf("Server %d: got AppendEntries from leader %d, return false 2.\n", 
				rf.me, args.LeaderId)
			reply.Term = rf.currentTerm
			reply.Success = false
			// fast append entry protocol
			reply.ConflictTerm = 0
			reply.ConflictIndex = len(rf.log) + rf.lastSnapshotIndex
			return
		}

		if rf.log[preLogOffsetIndex].Term != args.PrevLogTerm {
			DPrintf("Server %d: got AppendEntries from leader %d, return false 3.\n", 
				rf.me, args.LeaderId)
			reply.Term = rf.currentTerm
			reply.Success = false
			// fast append entry protocol
			reply.ConflictTerm = rf.log[preLogOffsetIndex].Term
			reply.ConflictIndex = rf.lastSnapshotIndex
			for termIndex := preLogOffsetIndex; termIndex > 0; termIndex-- {
				if rf.log[termIndex].Term == reply.ConflictTerm {
					reply.ConflictIndex = termIndex + rf.lastSnapshotIndex
				} else {
					break
				}
			}
			return		
		}
	} 

	if preLogOffsetIndex < 0 {
		DPrintf("Server %d: got AppendEntries return false 4 from leader %d.\n", 
			rf.me, args.LeaderId)
		// 当收到appendEntries后leader没收到响应，导致leader的nextIndex得不到更新，
		// 此时如果本server发生快照,就会进入到这个分支
		reply.Term = rf.currentTerm
		reply.Success = false
		// fast append entry protocol
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.commitIndex + 1
		return
	}

	unmatch_index := -1
	for idx := range args.Entries {
		if (preLogOffsetIndex + 1 + idx >= len(rf.log)) || 
			(rf.log[preLogOffsetIndex + 1 + idx].Term != args.Entries[idx].Term) {
			unmatch_index = idx
			break
		}
	}
	
	if unmatch_index != -1 {
		rf.log = append(rf.log[:preLogOffsetIndex+1+unmatch_index],
			args.Entries[unmatch_index:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+ rf.lastSnapshotIndex)
    }

	DPrintf("Server %d: got AppendEntries from leader %d, return true.\n", 
		rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("InstallSnapshot called, arg=%+v\n", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: got snapshot from leader %d, args: %+v, current term: %d, current commitIndex: %d, current log: %v\n", 
		rf.me, args.LeaderId, args, rf.currentTerm, rf.commitIndex, rf.log)
	
	if args.Term < rf.currentTerm {
		DPrintf("Server %d: got snapshot from leader %d, return false 1.\n", 
			rf.me, args.LeaderId)

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} 
	rf.cancelToCandidate = true
	rf.cancelToReCandidate = true
	rf.convertToFollower(args.Term, args.LeaderId)

	if args.LastIncludedIndex > rf.lastSnapshotIndex {

		offsetIncluedIndex := args.LastIncludedIndex - rf.lastSnapshotIndex
		if offsetIncluedIndex < len(rf.log) &&
			rf.log[offsetIncluedIndex].Index == args.LastIncludedIndex &&
			rf.log[offsetIncluedIndex].Term == args.LastIncludedTerm {
			rf.log = rf.log[offsetIncluedIndex:]
		} else {
			rf.log = []Entry{{rf.lastSnapshotIndex, rf.lastSnapshotTerm, ""}}
		}
		rf.lastSnapshotIndex = args.LastIncludedIndex
		rf.lastSnapshotTerm = args.LastIncludedTerm
		data := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(data, args.Data)
		
		// notify snapshot和apply entry都在同一线程，
		// 所以可以做到新的lastApplied生效前快照已经被notify上去了
		if rf.lastSnapshotIndex > rf.lastApplied {
			rf.snapshotToNotify = append(rf.snapshotToNotify, args)
			rf.commitIndex = Max(rf.commitIndex, rf.lastSnapshotIndex)
			rf.lastApplied = rf.lastSnapshotIndex
		} 


	}

	DPrintf("Server %d: got snapshot from leader %d, return true.\n", 
		rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = true
	return

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	term = rf.currentTerm
	isLeader = (rf.role == RoleLeader)

	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.log) + rf.lastSnapshotIndex
	rf.log = append(rf.log, Entry{index, rf.currentTerm, command})
    rf.matchIndex[rf.me] = index
    rf.nextIndex[rf.me] = index + 1
    rf.persist()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) notifyStartup() {
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.Type = MsgTypeStartup
	rf.applyCh <- msg
}

func (rf *Raft) notifyRole(role int) {
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.Type = MsgTypeRole
	msg.Role = role
	rf.applyCh <- msg
}

func (rf *Raft) notifyKill() {
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.Type = MsgTypeKill
	rf.applyCh <- msg
}

func (rf *Raft) notifySnapshot(snapshot []byte) {
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.Type = MsgTypeSnapshot
	msg.Snapshot = snapshot
	rf.applyCh <- msg
}

func (rf *Raft) convertToFollower(term int, voteFor int) {

	rf.role = RoleFollower
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.totalVotes = 0
	rf.persist()
	//rf.notifyRole()
}

func (rf *Raft) convertToCandidate() {

	rf.role = RoleCandidate
	rf.currentTerm ++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.persist()
	//rf.notifyRole()
}

func (rf *Raft) convertToLeader() {

	rf.role = RoleLeader
	for i:=0; i<len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + rf.lastSnapshotIndex
		rf.matchIndex[i] = 0
	}
	//rf.notifyRole()
}

func (rf *Raft) followLoop() {
	for {
		if rf.killed() {
			break
		}
		rf.cancelToCandidate = false
		time.Sleep(time.Duration(rf.timeoutToCandidate) * time.Millisecond)
		if rf.role != RoleFollower {
			continue
		}
		if rf.cancelToCandidate {
			continue;
		}
		rf.mu.Lock()
		rf.convertToCandidate()
		rf.mu.Unlock()
		go rf.startElection()
	}
}

func (rf *Raft) candidateLoop() {
	for {
		if rf.killed() {
			break
		}
		rf.cancelToReCandidate = false
		time.Sleep(time.Duration(rf.timeoutToReCandidate) * time.Millisecond)
		if rf.role != RoleCandidate {
			continue
		}
		if rf.cancelToReCandidate {
			continue;
		}
		rf.mu.Lock()
		rf.convertToCandidate()
		rf.mu.Unlock()
		go rf.startElection()
	}
}

func (rf *Raft) leaderLoop() {
	for {
		if rf.killed() {
			break
		}
		time.Sleep(time.Duration(rf.timeoutToHeartbeat) * time.Millisecond)
		if rf.role != RoleLeader {
			continue
		}

		rf.startAppendEntries()
	}
}

func (rf *Raft) CheckAndResetStartupCommitIndex() {
	if rf.startupCommitIndex >= 0 && rf.lastApplied >= rf.startupCommitIndex {
		rf.notifyStartup()
		rf.startupCommitIndex = -1
    }	
}

func (rf *Raft) applyToCommit() {
	if rf.lastApplied < rf.lastSnapshotIndex {
		rf.lastApplied = rf.lastSnapshotIndex
	}


    for rf.lastApplied < rf.commitIndex {
      rf.lastApplied++
      lastAppliedOffsetIndex := rf.lastApplied - rf.lastSnapshotIndex
      msg := ApplyMsg{}
      msg.CommandValid = true
      msg.CommandIndex = rf.lastApplied
      msg.CommandTerm = rf.currentTerm
      msg.Command = rf.log[lastAppliedOffsetIndex].Command
      rf.applyCh <- msg
      DPrintf("applyEntry, msg:%+v\n", msg)

      rf.CheckAndResetStartupCommitIndex()
    }
}


func (rf *Raft) applyLoop() {
  preRole := rf.role

  for {
	if rf.killed() {
		rf.notifyKill()
		break
	}
    time.Sleep(time.Duration(rf.timeoutToApplyLog) * time.Millisecond)

    // update role
    curRole := rf.role
    if preRole != curRole {
    	preRole = curRole
    	rf.notifyRole(curRole)
    }

    rf.mu.Lock()
    if (len(rf.snapshotToNotify) > 0) {
	    latestSnapshot := rf.snapshotToNotify[len(rf.snapshotToNotify)-1]
	    rf.notifySnapshot(latestSnapshot.Data)
	    rf.lastApplied = latestSnapshot.LastIncludedIndex 
	    rf.snapshotToNotify = []*InstallSnapshotArgs{}
    }

    rf.CheckAndResetStartupCommitIndex()

    // 必须放到快照通知之后，这样lastApplied才是正确的
    if rf.lastApplied < rf.commitIndex {
	    rf.applyToCommit()
    }

    rf.mu.Unlock()

  }
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleCandidate {
	  return
	}

	prepareFinish := make(chan bool)
	for peerId := range rf.peers {
	  if peerId == rf.me {
	  	continue
	  }

	  go func(id int) {
	  	term := rf.currentTerm
	  	candidateId := rf.me
	  	lastLogOffsetIndex := len(rf.log) - 1
	  	lastLogTerm := rf.log[lastLogOffsetIndex].Term 

	    args := RequestVoteArgs{
	    	Term: term,
	    	CandidateId: candidateId,
	    	LastLogIndex: lastLogOffsetIndex + rf.lastSnapshotIndex,
	    	LastLogTerm: lastLogTerm,
	    }
	    prepareFinish <- true
	    reply := RequestVoteReply{}
	    ok := rf.sendRequestVote(id, &args, &reply)
	    DPrintf("me(%d) sendRequestVote(%d) result:%+v,%+v,%+v\n", rf.me, id, ok, args, reply)
	    if !ok {
	    	DPrintf("Leader %d: sending RequestVote to server %d failed\n", rf.me, id)
	    } else {
	    	rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != RoleCandidate {
				// nothing
			} else if term != rf.currentTerm {
				// nothing
			} else if reply.Term > rf.currentTerm {
				DPrintf("Leader %d: turn back to follower due to existing higher term %d from server %d\n", rf.me, reply.Term, id)
				rf.convertToFollower(reply.Term, -1)
			} else if reply.Term < rf.currentTerm {
				// nothing
			} else if reply.VoteGranted {
				rf.totalVotes++
				if rf.totalVotes > len(rf.peers)/2 {
					rf.convertToLeader()
				}
			} 
	    }
	    // [send rpc] -> [wait] ->[handle the reply]
	  }(peerId)

	  <-prepareFinish
	}
}


func (rf *Raft) doSendInstallSnapshot(id int, args *InstallSnapshotArgs) {

    reply := &InstallSnapshotReply{}
    ok := rf.sendInstallSnapshot(id, args, reply)
    DPrintf("me(%d) sendInstallSnapshot(%d) result:%+v,%+v,%+v\n", rf.me, id, ok, args, reply)
    if !ok {
    	DPrintf("Leader %d: sending InstallSnapshot to server %d failed\n", rf.me, id)
    } else {
    	rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != RoleLeader {
			// nothing
		} else if args.Term != rf.currentTerm {
			// nothing
		} else if reply.Term > rf.currentTerm {
			DPrintf("Leader %d: turn back to follower due to existing higher term %d from server %d\n", rf.me, reply.Term, id)
			rf.convertToFollower(reply.Term, -1)
		} else if reply.Term < rf.currentTerm {
			// nothing
		} else if reply.Success {
			newNextIndex := args.LastIncludedIndex + 1
			if newNextIndex <= rf.nextIndex[id] {
				// nothing
			} else {
				rf.matchIndex[id] = newNextIndex - 1
				rf.nextIndex[id] = newNextIndex;
				myLogIndex := len(rf.log) - 1 + rf.lastSnapshotIndex
				rf.matchIndex[rf.me] = myLogIndex
			}
		} else {
			DPrintf("Leader %d: send snapshot to server %d not success.\n", rf.me, id)
		}

	}
}


func (rf *Raft) doSendAppendEntries(id int, args *AppendEntriesArgs) {

	    reply := &AppendEntriesReply{}
	    ok := rf.sendAppendEntries(id, args, reply)
	    DPrintf("me(%d) sendAppendEntries(%d) result:%+v,%+v,%+v\n", rf.me, id, ok, args, reply)
	    if !ok {
	    	DPrintf("Leader %d: sending AppendEntries to server %d failed\n", rf.me, id)
	    } else {
	    	rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != RoleLeader {
				// nothing
			} else if args.Term != rf.currentTerm {
				// nothing
			} else if reply.Term > rf.currentTerm {
				DPrintf("Leader %d: turn back to follower due to existing higher term %d from server %d\n", rf.me, reply.Term, id)
				rf.convertToFollower(reply.Term, -1)
			} else if reply.Term < rf.currentTerm {
				// nothing
			} else if reply.Success {
				newNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
				if newNextIndex <= rf.nextIndex[id] {
					// nothing
				} else {
					rf.matchIndex[id] = newNextIndex - 1
					rf.nextIndex[id] = newNextIndex;
					myLogIndex := len(rf.log) - 1 + rf.lastSnapshotIndex
					rf.matchIndex[rf.me] = myLogIndex

		            for biggerCommitIndex := myLogIndex; biggerCommitIndex > rf.commitIndex; biggerCommitIndex-- {
		                indexCount := 0
		                for _, matchIndex := range rf.matchIndex {
		                    if matchIndex >= biggerCommitIndex {
		                        indexCount++
		                    }
		                }
		                biggerCommitOffsetIndex := biggerCommitIndex - rf.lastSnapshotIndex
		                if indexCount > len(rf.peers)/2 && 
		                	rf.log[biggerCommitOffsetIndex].Term == rf.currentTerm{
		                    rf.commitIndex = biggerCommitIndex
		                    break
		                }
		            }

				}
			} else if rf.nextIndex[id] > 1 {
				solution := 2
				if solution==1 {
					// TestFigure8Unreliable2C对快速AppendEntry有要求
					// 一次后退10格加速匹配, 是最简单的解决这个问题的方案
					// 也可以用下面else中论文里的方案
					rf.nextIndex[id] -= 10;
					if rf.nextIndex[id] < 1 {
						rf.nextIndex[id] = 1
					}
				} else {
					// fast append entry protocol
					DPrintf("startAppendEntries me(%d)->(%d) nextid(%d),req:%+v, res:%+v, not success.\n", 
							rf.me, id, rf.nextIndex[id], args, reply)
					conflictIndex := reply.ConflictIndex
					conflictTerm := reply.ConflictTerm
					if conflictTerm > 0 {
						newNextIndex := 0
						for i:= args.PrevLogIndex; i > rf.lastSnapshotIndex; i-- {
							offsetI := i - rf.lastSnapshotIndex
							if rf.log[offsetI].Term > conflictTerm {
								newNextIndex = i
							} else {
								break
							}
						}
						if newNextIndex > 0 {
							rf.nextIndex[id] = newNextIndex
						} else {
							rf.nextIndex[id] = conflictIndex
						}
					} else {
						rf.nextIndex[id] = conflictIndex
					}
				}
			}
	    }
}

func (rf *Raft) startAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleLeader {
	  return
	}

	prepareFinish := make(chan bool)
	for peerId := range rf.peers {
	  if peerId == rf.me {
	  	continue
	  }

	  go func(id int) {
	    // prepare the append entries arguments
	    //fmt.Printf("in startAppendEntries, id=%d, rf=%+v\n\n\n\n", id, rf)
	    prevLogIndex := rf.nextIndex[id] - 1
	    currNextIndex := rf.nextIndex[id]
	    term := rf.currentTerm
	    commitIndex := rf.commitIndex
	    lerderId := rf.me


	    if (currNextIndex <= rf.lastSnapshotIndex) && 
	    	(rf.lastSnapshotIndex > 0) {
			args := InstallSnapshotArgs{
				Term: term,
				LeaderId: lerderId,
				LastIncludedIndex: rf.lastSnapshotIndex,
				LastIncludedTerm: rf.lastSnapshotTerm,
				Data: rf.persister.ReadSnapshot(),
			}
			prepareFinish <- true
			rf.doSendInstallSnapshot(id, &args)
	    } else {
		    prevLogOffsetIndex := prevLogIndex - rf.lastSnapshotIndex
		    prevLogTerm := rf.log[prevLogOffsetIndex].Term
		    currNextOffsetIndex := currNextIndex - rf.lastSnapshotIndex
		    entries := rf.log[currNextOffsetIndex:]

		    args := AppendEntriesArgs{
		    	Term: term,
		    	LeaderId: lerderId,
		    	PrevLogTerm: prevLogTerm,
		    	PrevLogIndex: prevLogIndex,
		    	Entries: entries,
		    	LeaderCommit: commitIndex,
		    }
		    prepareFinish <- true
		    rf.doSendAppendEntries(id, &args)
	    }


	    // [send rpc] -> [wait] ->[handle the reply]
	  }(peerId)

	  <-prepareFinish
	}

}

/*
func (rf *Raft) String() string {
  return fmt.Sprintf("[%d:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
    rf.role, rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastApplied)
}
*/

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	//
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Entry{{}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	//rf.nextIndex = ?
	//rf.matchIndex = ?
	rf.role = RoleFollower
	rf.applyCh = applyCh
	rf.timeoutToCandidate = GenerateElectionTimeout(150,300)
	rf.timeoutToReCandidate = rf.timeoutToCandidate
	rf.timeoutToHeartbeat = 50
	rf.timeoutToApplyLog = 10
	rf.snapshotToNotify = []*InstallSnapshotArgs{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.startupCommitIndex = rf.commitIndex
	//
	go rf.followLoop()
	go rf.candidateLoop()
	go rf.leaderLoop()
	go rf.applyLoop()
	//
	return rf
}
