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
// tester) on the same peer, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate RaftState = "Candidate"
	Leader    RaftState = "Leader"
)

const null int = -1

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
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
	CommandTerm  int
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type SnapShot struct {
	AppState          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

type Raft struct {
	mu        sync.RWMutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	CurrentTerm int
	VoteFor     int
	Entry       []LogEntry
	raftState   RaftState

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyChannel chan ApplyMsg
	applyCond    *sync.Cond

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func (rf *Raft) HeartBeatTimeOut() time.Duration {
	return 100 * time.Millisecond
}

func (rf *Raft) ElectionTimeOut() time.Duration {
	rand.Seed(time.Now().Unix() + int64(rf.me))
	return time.Duration(rand.Intn(350)+150) * time.Millisecond

}

func (rf *Raft) getRaftState() RaftState {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	rs := rf.raftState
	return rs
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	ct := rf.CurrentTerm
	return ct
}

func (rf *Raft) getBaseIndex() int {
	return rf.Entry[0].Index
}

func (rf *Raft) getMatchNextIndex(peer int) (matchindex int, nextindex int) {
	matchindex = rf.matchIndex[peer]
	nextindex = rf.nextIndex[peer]
	return
}

func (rf *Raft) UpdateMNIndex(peer, match, next int) {
	if match != null {
		rf.matchIndex[peer] = match
	}
	if next != null {
		rf.nextIndex[peer] = next
	}
}

func (rf *Raft) GetState() (int, bool) {
	term := rf.getCurrentTerm()
	isleader := (rf.getRaftState() == Leader)
	return term, isleader
}

func (rf *Raft) EncodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Entry)
	data := w.Bytes()
	return data
}

func (rf *Raft) Persist() {
	rf.persister.SaveRaftState(rf.EncodeRaftState())
}

func (rf *Raft) ReadPersist(data []byte) {
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
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VoteFor) != nil || d.Decode(&Entry) != nil {
		log.Fatalf("Decode Error\n")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor
		rf.Entry = Entry
		rf.commitIndex = Entry[0].Index
		rf.lastApplied = Entry[0].Index
	}

}

func (rf *Raft) ResetTimer(isleader bool) {
	if isleader {
		rf.heartbeatTimer.Reset(rf.HeartBeatTimeOut())
	} else {
		rf.electionTimer.Reset(rf.ElectionTimeOut())
	}
}

//
// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
// If it finds an entry in its log with that term:
// 		it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
// If it does not find an entry with that term:
// 		it should set nextIndex = conflictIndex.
//
func (rf *Raft) FindNextIndex(ConflictIndex, ConflictTerm, prevLogIndex int) int {
	BaseIndex := rf.getBaseIndex()
	if ConflictTerm == 0 || ConflictIndex < BaseIndex || rf.Entry[ConflictIndex-BaseIndex].Term == ConflictTerm {
		return ConflictIndex
	}
	left, right := BaseIndex, prevLogIndex

	if right < left {
		return left
	}

	for left < right-1 {
		mid := left + (right-left)/2
		if rf.Entry[mid-BaseIndex].Term <= ConflictTerm {
			left = mid
		} else {
			right = mid - 1
		}
	}

	if rf.Entry[right-BaseIndex].Term == ConflictTerm {
		return right + 1
	} else if rf.Entry[left-BaseIndex].Term == ConflictTerm {
		return left + 1
	}

	return ConflictIndex
}

//
// If there exists an N such that N > commitIndex,
// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// 		set commitIndex = N
//
func (rf *Raft) FindN() {
	matchIndexSet := make([]int, 0)
	BaseIndex := rf.getBaseIndex()
	for peer := range rf.peers {
		if peer != rf.me {
			matchindex, _ := rf.getMatchNextIndex(peer)
			matchIndexSet = append(matchIndexSet, matchindex)
		}
	}
	sort.Ints(matchIndexSet)
	N := matchIndexSet[len(rf.peers)/2]

	if N < BaseIndex {
		return
	}

	if N > rf.commitIndex && rf.Entry[N-BaseIndex].Term == rf.CurrentTerm {
		rf.UpdateCommitAndApply(N)
	}
}

func (rf *Raft) UpdateCommitAndApply(commitIndex int) {
	send := false
	if rf.commitIndex < commitIndex {
		rf.commitIndex = commitIndex
		send = true
	}

	if send {
		DPrintf("[%d] update commitindex at %d", rf.me, commitIndex)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) GetRequestVoteArg() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Entry) + rf.getBaseIndex() - 1,
		LastLogTerm:  rf.Entry[len(rf.Entry)-1].Term,
	}
	return args
}

func (rf *Raft) GetAppendEntriesArg(prevLogIndex int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.Entry[prevLogIndex-rf.getBaseIndex()].Term,
		LeaderCommit: rf.commitIndex,
	}

	for idx := prevLogIndex - rf.getBaseIndex() + 1; idx <= len(rf.Entry)-1; idx++ {
		args.Entry = append(args.Entry, rf.Entry[idx])
	}
	return args
}

func (rf *Raft) GetSnapshotArg() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LastIncludedIndex: rf.getBaseIndex(),
		LastIncludedTerm:  rf.Entry[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
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

func (rf *Raft) ChangeState(targetState RaftState) {
	rf.raftState = targetState
	// DPrintf("[%d] become %v", rf.me, targetState)
}

//
// PART: Election
//
func (rf *Raft) StartElection() {
	voteCnt := 1
	args := rf.GetRequestVoteArg()

	for peer := range rf.peers {
		if peer != rf.me {
			// DPrintf("[%d] send requestVoteRPC to %d", rf.me, peer)
			go func(peer int) {
				reply := new(RequestVoteReply)
				if rf.SendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Grant {
						voteCnt++
						if voteCnt > len(rf.peers)/2 && rf.raftState == Candidate && args.Term == rf.CurrentTerm {
							rf.ChangeState(Leader)

							next := len(rf.Entry) + rf.getBaseIndex()
							rf.ResetTimer(true)
							for peer := range rf.peers {
								rf.UpdateMNIndex(peer, 0, next)
							}
							rf.StartHeartbeat()
						}
					} else if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.VoteFor = null
						rf.CurrentTerm = reply.Term
						rf.ChangeState(Follower)
						rf.Persist()
					}
				}
			}(peer)
		}
	}
}

func (rf *Raft) SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	for {
		if rf.getCurrentTerm() != args.Term || rf.getRaftState() != Candidate {
			// DPrintf("[%d] sendRequestVote to %d fail for state changed", rf.me, peer)
			return false
		}
		if rf.peers[peer].Call("Raft.RequestVote", args, reply) {
			// DPrintf("[%d] get %+v and Grant %v", peer, args, reply.Grant)
			return true
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	if args.Term < rf.CurrentTerm || (args.Term == rf.CurrentTerm && (rf.VoteFor != null && rf.VoteFor != args.CandidateId)) {
		reply.Term = rf.CurrentTerm
		reply.Grant = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.VoteFor = null
		rf.CurrentTerm = reply.Term
		rf.ChangeState(Follower)
	}

	LastLogTerm := rf.Entry[len(rf.Entry)-1].Term
	LastLogIndex := len(rf.Entry) + rf.getBaseIndex() - 1

	if args.LastLogTerm < LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogIndex < LastLogIndex) {
		reply.Term = rf.CurrentTerm
		reply.Grant = false
		return
	}

	reply.Term = rf.CurrentTerm
	reply.Grant = true
	rf.VoteFor = args.CandidateId
	rf.ResetTimer(false)
}

//
// PART: Heartbeat
//
func (rf *Raft) StartHeartbeat() {
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.SendHeartBeat(peer)
		}
	}
}

func (rf *Raft) SendHeartBeat(peer int) {
	rf.mu.RLock()
	if rf.raftState != Leader {
		rf.mu.RUnlock()
		return
	}

	_, nextindex := rf.getMatchNextIndex(peer)
	prevLogIndex := nextindex - 1

	if prevLogIndex < rf.getBaseIndex() {
		SnapshotArgs := rf.GetSnapshotArg()
		SnapshotReply := new(InstallSnapshotReply)
		rf.mu.RUnlock()

		if rf.SendInstallSnapshot(peer, SnapshotArgs, SnapshotReply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if SnapshotReply.Term > rf.CurrentTerm {
				rf.VoteFor = null
				rf.CurrentTerm = SnapshotReply.Term
				rf.ChangeState(Follower)
				rf.Persist()
				return
			}
			rf.UpdateMNIndex(peer, rf.getBaseIndex(), rf.getBaseIndex()+1)
		}
	} else {
		AppendEntriesArgs := rf.GetAppendEntriesArg(prevLogIndex)
		AppendEntriesReply := new(AppendEntriesReply)
		rf.mu.RUnlock()

		if rf.SendAppendEntries(peer, AppendEntriesArgs, AppendEntriesReply) {
			DPrintf("[%d] send %+v to %d", rf.me, AppendEntriesArgs, peer)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if AppendEntriesReply.Term > rf.CurrentTerm {
				rf.VoteFor = null
				rf.CurrentTerm = AppendEntriesReply.Term
				rf.ChangeState(Follower)
				rf.Persist()
				return
			}

			if AppendEntriesReply.Success {
				rf.UpdateMNIndex(peer, AppendEntriesReply.ConflictIndex, AppendEntriesReply.ConflictIndex+1)
				rf.FindN()
				return
			}
			next := rf.FindNextIndex(AppendEntriesReply.ConflictIndex, AppendEntriesReply.ConflictTerm, AppendEntriesArgs.PrevLogIndex)
			rf.UpdateMNIndex(peer, null, next)
		}
	}
}

func (rf *Raft) SendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitindex := rf.commitIndex
		baseindex := rf.getBaseIndex()
		entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.Entry[rf.lastApplied+1-baseindex:rf.commitIndex+1-baseindex])
		DPrintf("[%d] apply len(entry): %d baseindex: %d lastapplied: %d commit: %d", rf.me, len(rf.Entry), baseindex, rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyChannel <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitindex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) SendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	if args.Term < rf.CurrentTerm {
		reply.Term, reply.Success = rf.CurrentTerm, false
		DPrintf("[%d] reject %d for old term", rf.me, args.LeaderId)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm, rf.VoteFor = args.Term, null
	}

	rf.ChangeState(Follower)
	rf.ResetTimer(false)

	BaseIndex := rf.getBaseIndex()

	if args.PrevLogIndex < BaseIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictIndex = BaseIndex
		reply.ConflictTerm = rf.Entry[0].Term
		DPrintf("[%d] reject %d for PrevLogIndex %d < BaseIndex %d", rf.me, args.LeaderId, args.PrevLogIndex, BaseIndex)
		return
	}

	reply.Term = rf.CurrentTerm
	// If a follower does not have prevLogIndex in its log:
	// 		it should return with conflictIndex = len(log) and conflictTerm = None.
	if args.PrevLogIndex-BaseIndex >= len(rf.Entry) {
		reply.Success, reply.ConflictIndex, reply.ConflictTerm = false, len(rf.Entry)+BaseIndex, 0
		DPrintf("[%d] reject %d for PrevLogIndex %d > LastIndex %d", rf.me, args.LeaderId, args.PrevLogIndex, BaseIndex+len(rf.Entry)-1)
		return
	}

	// If a follower does have prevLogIndex in its log, but the term does not match:
	// 		it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
	if rf.Entry[args.PrevLogIndex-BaseIndex].Term != args.PrevLogTerm {
		ConflictTerm := rf.Entry[args.PrevLogIndex-BaseIndex].Term
		// search its log for the first index whose entry has term equal to conflictTerm.
		reply.Success = false
		reply.ConflictIndex =
			func(left, right int) int {
				for left < right-1 {
					mid := left + (right-left)/2
					if rf.Entry[mid-BaseIndex].Term >= ConflictTerm {
						right = mid // right bound compact
					} else {
						left = mid + 1 // left bound compact
					}
				}
				if rf.Entry[left-BaseIndex].Term != ConflictTerm {
					return right
				}
				return left
			}(BaseIndex, args.PrevLogIndex)
		reply.ConflictTerm = ConflictTerm
		DPrintf("[%d] reject %d for Term %d != PrevLogTerm %d", rf.me, args.LeaderId, rf.Entry[args.PrevLogIndex-BaseIndex].Term, args.PrevLogTerm)
		return
	}

	// prevlog matches
	reply.ConflictIndex = args.PrevLogIndex
	reply.Success = true
	if len(args.Entry) != 0 {
		idx := 1
		for args.PrevLogIndex-BaseIndex+idx <= len(rf.Entry)-1 && idx <= len(args.Entry) {
			if rf.Entry[args.PrevLogIndex-BaseIndex+idx].Term != args.Entry[idx-1].Term {
				rf.Entry = rf.Entry[:args.PrevLogIndex-BaseIndex+idx]
				break
			}
			idx++
		}
		for idx <= len(args.Entry) {
			rf.Entry = append(rf.Entry, args.Entry[idx-1])
			idx++
		}
		reply.ConflictIndex = len(rf.Entry) - 1 + BaseIndex
	}

	// DPrintf("[%d] Append Success with conflictIndex %d", rf.me, reply.ConflictIndex)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.UpdateCommitAndApply(min(args.LeaderCommit, len(rf.Entry)-1+BaseIndex))
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm, rf.VoteFor = args.Term, null
	}

	rf.ChangeState(Follower)
	rf.ResetTimer(false)
	reply.Term = rf.CurrentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	BaseIndex := rf.getBaseIndex()
	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if args.LastIncludedIndex-BaseIndex < len(rf.Entry) && args.LastIncludedIndex > BaseIndex {
		rf.Entry = rf.Entry[args.LastIncludedIndex-BaseIndex:]
	} else {
		rf.Entry = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	}

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persister.SaveSnapshot(args.Data)

	DPrintf("[%d] baseindex: %d lastallplied %d commitindex %d after installsnapshot", rf.me, BaseIndex, rf.lastApplied, rf.commitIndex)

	go func() {
		rf.applyChannel <- ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
	}()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	BaseIndex := rf.getBaseIndex()

	if index <= BaseIndex {
		DPrintf("[%d] Snapshot fail\n", rf.me)
		return
	}

	DPrintf("[%d] len(entry): %d baseindex: %d lastapplied: %d commit: %d", rf.me, len(rf.Entry), BaseIndex, rf.lastApplied, rf.commitIndex)
	rf.Entry = rf.Entry[index-BaseIndex:]
	rf.persister.SaveStateAndSnapshot(rf.EncodeRaftState(), snapshot)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.CurrentTerm
	isleader := (rf.raftState == Leader)

	index := len(rf.Entry) + rf.Entry[0].Index
	if isleader {
		Log := LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		DPrintf("[%d] START %+v at {%d}\n", rf.me, Log, index)
		rf.Entry = append(rf.Entry, Log)
		rf.Persist()
		rf.StartHeartbeat()
	}

	return index, term, isleader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("[%d] dead", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.raftState != Leader {
				rf.CurrentTerm++
				rf.VoteFor = rf.me
				rf.ChangeState(Candidate)
				rf.Persist()
				// DPrintf("[%d] candidate term : %d", rf.me, rf.CurrentTerm)

				rf.StartElection()
				rf.ResetTimer(false)
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.raftState == Leader {
				rf.ResetTimer(true)
				rf.StartHeartbeat()
			}
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		CurrentTerm: 0,
		VoteFor:     null,
		Entry:       []LogEntry{{0, 0, 0}},
		raftState:   Follower,

		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),

		applyChannel: applyCh,
		// replicatorCond: make([]*sync.Cond, len(peers)),

		electionTimer:  time.NewTimer(0),
		heartbeatTimer: time.NewTimer(0),
	}

	rf.ReadPersist(persister.ReadRaftState())
	rf.ResetTimer(false)
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash

	go rf.Ticker()
	go rf.Applier()
	DPrintf("[%d] make success", rf.me)

	return rf
}
