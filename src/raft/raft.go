package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

const null int = -1

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// instead, we suggest that you simply have it return true.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type SnapShot struct {
	AppState          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.RWMutex // Lock to protect shared access to this peer's state
	indexSliceMu []sync.RWMutex
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()

	CurrentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VoteFor     int        // candidateId that received vote in current term (or null if none)
	Entry       []LogEntry // log[index-1] -> {term, command}
	raftState   RaftState

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyChannel chan ApplyMsg

	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer
	electionTimeout int

	BaseIndex int
	snapshot  *SnapShot

	updateApplyCh chan int
	updateApplyMu sync.RWMutex
}

func (rf *Raft) RaftState() RaftState {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	rs := rf.raftState
	return rs
}

func (rf *Raft) currentTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	ct := rf.CurrentTerm
	return ct
}

func (rf *Raft) LogEntry() (Entry []LogEntry, length int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	length = len(rf.Entry)
	Entry = make([]LogEntry, length)
	copy(Entry, rf.Entry)
	return
}

func (rf *Raft) MatchNextIndex(server int) (matchindex int, nextindex int) {
	rf.indexSliceMu[server].RLock()
	defer rf.indexSliceMu[server].RUnlock()
	nextindex = rf.nextIndex[server]
	matchindex = rf.matchIndex[server]
	return
}

func (rf *Raft) UpdateMNIndex(server, match, next int) {
	rf.indexSliceMu[server].Lock()
	defer rf.indexSliceMu[server].Unlock()
	if match != -1 {
		rf.matchIndex[server] = match
	}
	if next != -1 {
		rf.nextIndex[server] = next
	}
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm()
	isleader := (rf.RaftState() == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Entry)
	e.Encode(rf.BaseIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// rf.persister.SaveStateAndSnapshot(nil, rf.snapshot.AppState)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var CurrentTerm int
	var VoteFor int
	var Entry []LogEntry
	var BaseIndex int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VoteFor) != nil || d.Decode(&Entry) != nil || d.Decode(&BaseIndex) != nil {
		log.Fatalf("Decode Error\n")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor
		rf.Entry = Entry
		rf.BaseIndex = BaseIndex
		rf.commitIndex = BaseIndex

		rf.updateApplyMu.Lock()
		rf.lastApplied = BaseIndex
		rf.updateApplyMu.Unlock()
	}

}

//
// reset the Timer
//
func (rf *Raft) resetTimer(isleader bool) {
	// fmt.Println(rf.me, "resetTimer ", isleader)
	if isleader {
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
	} else {
		rand.Seed(time.Now().Unix() + int64(rf.me))
		rf.electionTimeout = rand.Intn(500) + 500
		rf.electionTimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	}
}

//
// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
// If it finds an entry in its log with that term:
// 		it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
// If it does not find an entry with that term:
// 		it should set nextIndex = conflictIndex.
//
func (rf *Raft) findNextIndex(ConflictIndex, ConflictTerm, prevLogIndex int) int {
	if ConflictTerm == 0 || rf.Entry[ConflictIndex-rf.BaseIndex].Term == ConflictTerm {
		return ConflictIndex
	}
	left, right := rf.BaseIndex+1, prevLogIndex

	for left < right-1 {
		mid := left + (right-left)/2
		if rf.Entry[mid-rf.BaseIndex].Term <= ConflictTerm {
			left = mid
		} else {
			right = mid - 1
		}
	}

	if rf.Entry[right-rf.BaseIndex].Term == ConflictTerm {
		return right + 1
	} else if rf.Entry[left-rf.BaseIndex].Term == ConflictTerm {
		return left + 1
	}

	return ConflictIndex
}

func (rf *Raft) UpdateCommitAndApply(commitIndex int) {
	// fmt.Println(rf.me, "update", commitIndex)
	rf.commitIndex = commitIndex
	rf.updateApplyCh <- commitIndex
}

func (rf *Raft) UpdateApplyThread() {
	for {
		select {
		case commitidx := <-rf.updateApplyCh:
			rf.mu.RLock()
			entries := make([]LogEntry, len(rf.Entry))
			copy(entries, rf.Entry)
			baseindex := rf.BaseIndex
			rf.mu.RUnlock()

			rf.updateApplyMu.RLock()
			lastapplied := rf.lastApplied
			rf.updateApplyMu.RUnlock()

			for lastapplied < commitidx {
				lastapplied++
				rf.updateApplyMu.Lock()
				rf.lastApplied = lastapplied
				rf.updateApplyMu.Unlock()
				rf.applyChannel <- ApplyMsg{
					CommandValid: true,
					Command:      entries[rf.lastApplied-baseindex].Command,
					CommandIndex: rf.lastApplied,
				}
				// fmt.Println(rf.me, ":", entries[rf.lastApplied-baseindex], "is applied")
			}

			// fmt.Println("commit idx :", commitidx)
		}
	}
}

//
// If there exists an N such that N > commitIndex,
// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// 		set commitIndex = N
//
func (rf *Raft) findN() {
	matchIndexSet := make([]int, 0)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		matchindex, _ := rf.MatchNextIndex(i)
		matchIndexSet = append(matchIndexSet, matchindex)
	}
	sort.Ints(matchIndexSet)
	N := matchIndexSet[len(rf.peers)/2]

	// fmt.Println("#", N, rf.commitIndex, rf.CurrentTerm)
	if N > rf.commitIndex && rf.Entry[N-rf.BaseIndex].Term == rf.CurrentTerm {
		rf.UpdateCommitAndApply(N)
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entry        []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  // CurrentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // log index in case of conflict
	ConflictTerm  int  // The term corresponding to the log index in case of the conflict
}

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term  int
	Grant bool
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	rf.raftState = Leader
	// fmt.Printf("[%d] become Leader at Term : %d\n", rf.me, rf.CurrentTerm)
	next := len(rf.Entry) + rf.BaseIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.UpdateMNIndex(i, 0, next)
	}
	rf.resetTimer(true)
	rf.mu.Unlock()

	rf.sendHeartBeat()
}

//
// • Increment CurrentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
//
func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VoteFor = rf.me
	rf.raftState = Candidate
	rf.resetTimer(false)
	rf.persist()
	// fmt.Printf("[%d] become Candidate at Term : %d time: %v\n", rf.me, rf.CurrentTerm, time.Now())
	rf.mu.Unlock()

	rf.sendRequestVote()
}

func (rf *Raft) BecomeFollwer(term int) {
	if rf.CurrentTerm < term {
		rf.VoteFor = null
		rf.CurrentTerm = term
		rf.persist()
	}
	rf.raftState = Follower
	// fmt.Println("[", rf.me, "] Term:", rf.CurrentTerm, "become follower")
}

func (rf *Raft) sendRequestVote() {
	voteCnt := 1
	totalCnt := uint32(1)
	Entry, length := rf.LogEntry()
	currentTime := time.Now()

	rf.mu.RLock()
	term := rf.CurrentTerm
	timeout := rf.electionTimeout
	rf.mu.RUnlock()

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: length + rf.BaseIndex - 1,
			LastLogTerm:  Entry[length-1].Term,
		}

		go func(server int) {
			// fmt.Printf("[%d] send requestvoteRPC to %d\n", rf.me, server)
			reply := &RequestVoteReply{}
			for {
				if rf.RaftState() != Candidate || time.Since(currentTime) >= time.Duration(timeout)*time.Millisecond {
					// fmt.Printf("[%d] Time limit exceeded\n", rf.me)
					atomic.AddUint32(&totalCnt, 1)
					break
				}

				ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
				if ok {
					atomic.AddUint32(&totalCnt, 1)
					if reply.Grant {
						mu.Lock()
						defer mu.Unlock()
						voteCnt++
						// fmt.Printf("[%d] get grant from %d\n", rf.me, server)
					} else if reply.Term > term {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.BecomeFollwer(reply.Term)
						return
					}
					break
				}
			}
			cond.Signal()
		}(i)
	}

	mu.Lock()
	defer mu.Unlock()
	for rf.RaftState() == Candidate && voteCnt <= len(rf.peers)/2 && int(atomic.LoadUint32(&totalCnt)) != len(rf.peers) {
		cond.Wait()
	}

	if rf.RaftState() != Candidate {
		return
	}

	if voteCnt > len(rf.peers)/2 {
		rf.BecomeLeader()
	} else {
		rf.mu.Lock()
		rf.raftState = Follower
		rf.mu.Unlock()
	}

	// fmt.Printf("[%d] SendRequestVote over...\n", rf.me)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Grant = true
	if args.Term > rf.CurrentTerm {
		rf.BecomeFollwer(args.Term)
	} else if args.Term < rf.CurrentTerm {
		// Reply false if term < CurrentTerm
		// fmt.Println(rf.me, "deny", args.CandidateId, "for old term")
		reply.Term = rf.CurrentTerm
		reply.Grant = false
		return
	}
	reply.Term = rf.CurrentTerm
	//
	// Election Restriction:
	// the RPC includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	// If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	//
	LastLogTerm := rf.Entry[len(rf.Entry)-1].Term
	LastLogIndex := len(rf.Entry) + rf.BaseIndex - 1

	//
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	//
	if rf.VoteFor == null || rf.VoteFor == args.CandidateId {
		if args.LastLogTerm < LastLogTerm {
			// fmt.Println(rf.me, "deny", args.CandidateId, "for args.LastLogTerm < LastLogTerm")
			reply.Grant = false
		} else if args.LastLogTerm == LastLogTerm && args.LastLogIndex < LastLogIndex {
			// fmt.Println(rf.me, "deny", args.CandidateId, "for args.LastLogIndex < LastLogIndex")
			reply.Grant = false
		}
	} else {
		// fmt.Println(rf.me, "deny", args.CandidateId, "for voted")
		reply.Grant = false
	}

	if reply.Grant {
		rf.VoteFor = args.CandidateId
		rf.persist()
		rf.resetTimer(false)
		// fmt.Println(rf.me, "grant", args.CandidateId)
	}
}

func (rf *Raft) sendHeartBeat() {
	// fmt.Printf("[%d] heartbeat\n", rf.me)
	// term, _ := rf.GetState()
	rf.mu.RLock()
	term := rf.CurrentTerm
	length := len(rf.Entry)
	Entry := make([]LogEntry, length)
	copy(Entry, rf.Entry)
	baseindex := rf.BaseIndex
	commitindex := rf.commitIndex
	rf.mu.RUnlock()
	// fmt.Printf("[%d] Entry: %v\n", rf.me, rf.Entry)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				LeaderCommit: commitindex,
			}
			reply := &AppendEntriesReply{}

			args.Entry = make([]LogEntry, 0)

			_, nextindex := rf.MatchNextIndex(server)
			args.PrevLogIndex = nextindex - 1

			if args.PrevLogIndex < baseindex { // send Snapshot
				SnapshotArgs := &InstallSnapshotArgs{
					Term:              term,
					LastIncludedIndex: baseindex,
					LastIncludedTerm:  Entry[0].Term,
					Data:              rf.persister.ReadSnapshot(),
				}

				SnapshotReply := &InstallSnapshotReply{}
				ok := rf.peers[server].Call("Raft.InstallSnapshot", SnapshotArgs, SnapshotReply)

				if ok {
					if SnapshotReply.Term > term {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.BecomeFollwer(reply.Term)
						return
					}
					// args.PrevLogIndex = rf.BaseIndex
					rf.UpdateMNIndex(server, -1, baseindex+1)
				}
				return
			}

			args.PrevLogTerm = Entry[args.PrevLogIndex-baseindex].Term
			for idx := nextindex - baseindex; idx <= length-1; idx++ {
				args.Entry = append(args.Entry, Entry[idx])
			}

			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if ok {
				if len(args.Entry) != 0 {
					// fmt.Printf("[%d] Entry: %v	BaseIndex: %d	Commit: %d	Apply: %d\n", rf.me, Entry, baseindex, commitindex, rf.LastApplied())
				}
				if reply.Term > term {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.BecomeFollwer(reply.Term)
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success {
					rf.UpdateMNIndex(server, reply.ConflictIndex, reply.ConflictIndex+1)
					rf.findN()
					return
				}
				rf.UpdateMNIndex(server, -1, rf.findNextIndex(reply.ConflictIndex, reply.ConflictTerm, args.PrevLogIndex))
			}
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = max(args.Term, rf.CurrentTerm)

	if args.Term < rf.CurrentTerm { // Reply false if term < CurrentTerm
		// fmt.Println("old term append rpc")
		reply.Success = false
		return
	}

	rf.BecomeFollwer(args.Term)
	//
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// If a follower does not have prevLogIndex in its log:
	// 		it should return with conflictIndex = len(log) and conflictTerm = None.
	// If a follower does have prevLogIndex in its log, but the term does not match:
	// 		it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
	//
	if args.PrevLogIndex-rf.BaseIndex >= len(rf.Entry) {
		reply.ConflictIndex = len(rf.Entry) + rf.BaseIndex
		reply.ConflictTerm = 0
		reply.Success = false
		return
	}
	if rf.Entry[args.PrevLogIndex-rf.BaseIndex].Term != args.PrevLogTerm {
		reply.ConflictIndex =
			func() int { // search its log for the first index whose entry has term equal to conflictTerm.
				left, right := rf.BaseIndex, args.PrevLogIndex
				for left < right-1 {
					mid := left + (right-left)/2
					if rf.Entry[mid-rf.BaseIndex].Term >= rf.Entry[args.PrevLogIndex-rf.BaseIndex].Term {
						right = mid // right bound compact
					} else {
						left = mid + 1 // left bound compact
					}
				}
				if rf.Entry[left-rf.BaseIndex].Term != rf.Entry[args.PrevLogIndex-rf.BaseIndex].Term {
					return right
				}
				return left
			}()
		reply.ConflictTerm = rf.Entry[args.PrevLogIndex-rf.BaseIndex].Term
		reply.Success = false
		return
	}

	reply.ConflictIndex = args.PrevLogIndex
	if len(args.Entry) != 0 { // NOT a HeartBeat
		idx := 1
		for args.PrevLogIndex-rf.BaseIndex+idx <= len(rf.Entry)-1 && idx <= len(args.Entry) {
			if rf.Entry[args.PrevLogIndex-rf.BaseIndex+idx].Term != args.Entry[idx-1].Term {
				//
				// If an existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it
				// Append any new entries not already in the Entry
				//
				rf.Entry = rf.Entry[:args.PrevLogIndex-rf.BaseIndex+idx]
				break
			}
			idx++
		}
		for idx <= len(args.Entry) {
			rf.Entry = append(rf.Entry, args.Entry[idx-1])
			idx++
		}
		reply.ConflictIndex = len(rf.Entry) - 1 + rf.BaseIndex
		rf.persist()
		// fmt.Printf("[%d] Entry: %v	BaseIndex: %d	Commit: %d	Apply: %d\n", rf.me, rf.Entry, rf.BaseIndex, rf.commitIndex, rf.LastApplied())
	}

	//
	// If leaderCommit > commitIndex,
	// 	set commitIndex = min(leaderCommit, index of last new entry)
	// If commitIndex > lastApplied: increment lastApplied,
	// 	apply log[lastApplied] to state machine
	//
	if args.LeaderCommit > rf.commitIndex {
		rf.UpdateCommitAndApply(min(args.LeaderCommit, len(rf.Entry)+rf.BaseIndex-1))
	}
	rf.resetTimer(false)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.BecomeFollwer(args.Term)
	}

	reply.Term = rf.CurrentTerm

	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if args.LastIncludedIndex-rf.BaseIndex < len(rf.Entry) {
		rf.Entry = rf.Entry[args.LastIncludedIndex-rf.BaseIndex:]
	} else {
		rf.Entry = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.BaseIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.persister.SaveSnapshot(args.Data)
	rf.mu.Unlock()

	rf.updateApplyMu.Lock()
	rf.lastApplied = args.LastIncludedIndex
	rf.updateApplyMu.Unlock()

	go func() {
		rf.applyChannel <- ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
	}()
}

//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.BaseIndex || index > rf.lastApplied {
		return
	}

	rf.Entry = rf.Entry[index-rf.BaseIndex:]
	rf.BaseIndex = index
	rf.persist()
	rf.persister.SaveSnapshot(snapshot)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isleader := rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.Entry) + rf.BaseIndex
	if isleader {
		Log := LogEntry{
			Term:    term,
			Command: command,
		}
		// fmt.Printf("----START %v----\n", Log)
		rf.Entry = append(rf.Entry, Log)
		rf.persist()
	}

	return index, term, isleader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			if _, isleader := rf.GetState(); !isleader {
				go rf.BecomeCandidate()
			}

		case <-rf.heartbeatTimer.C:
			if _, isleader := rf.GetState(); isleader {
				rf.resetTimer(true)
				go rf.sendHeartBeat()
			}
		}
	}
}

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
	rf := &Raft{
		peers:        peers,
		persister:    persister,
		me:           me,
		CurrentTerm:  0,
		VoteFor:      null,
		Entry:        []LogEntry{{0, 0}},
		raftState:    Follower,
		commitIndex:  0,
		lastApplied:  0,
		applyChannel: applyCh,
		BaseIndex:    0,

		electionTimer:  time.NewTimer(0),
		heartbeatTimer: time.NewTimer(0),

		updateApplyCh: make(chan int),
	}
	rf.indexSliceMu = make([]sync.RWMutex, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resetTimer(false)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.UpdateApplyThread()

	return rf
}
