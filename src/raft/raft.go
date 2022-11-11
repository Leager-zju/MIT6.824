package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// Debugging
// const Debug = true

const Debug = false

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate RaftState = "Candidate"
	Leader    RaftState = "Leader"
)

type ReplyErr string

const (
	OK                       ReplyErr = "OK"
	ErrOldTerm               ReplyErr = "ErrOldTerm"
	ErrLogNotMatch           ReplyErr = "ErrLogNotMatch"
	ErrVoted                 ReplyErr = "ErrVoted"
	ErrPrevLogIndexUnderFlow ReplyErr = "ErrPrevLogIndexUnderFlow"
	ErrPrevLogIndexOverFlow  ReplyErr = "ErrPrevLogIndexOverFlow"
	ErrOldSnapshot           ReplyErr = "ErrOldSnapshot"
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

// instead, we suggest that you simply have it return true.
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
	return time.Duration(rand.Intn(450)+250) * time.Millisecond
}

func (rf *Raft) GetRaftState() RaftState {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.raftState
}

func (rf *Raft) GetCurrentTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.CurrentTerm
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.CurrentTerm, rf.raftState == Leader
}

func (rf *Raft) GetBaseLog() LogEntry {
	return rf.Entry[0]
}

func (rf *Raft) GetLastLog() LogEntry {
	return rf.Entry[len(rf.Entry)-1]
}

func (rf *Raft) GetMatchNextIndex(peer int) (matchindex int, nextindex int) {
	return rf.matchIndex[peer], rf.nextIndex[peer]
}

func (rf *Raft) UpdateMatchNextIndex(peer, match, next int) {
	if match != null {
		rf.matchIndex[peer] = match
	}
	if next != null {
		DPrintf("[%d %d %v] nextIndex[%d] %d -> %d", rf.me, rf.CurrentTerm, rf.raftState, peer, rf.nextIndex[peer], next)
		rf.nextIndex[peer] = next
	}
}

func (rf *Raft) Persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Entry)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	DPrintf("[%d %d %v] Read Persist After Crash", rf.me, rf.CurrentTerm, rf.raftState)
}

func (rf *Raft) ResetTimer(isleader bool) {
	if isleader {
		rf.heartbeatTimer.Reset(rf.HeartBeatTimeOut())
	} else {
		rf.electionTimer.Reset(rf.ElectionTimeOut())
	}
}

// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
// If it finds an entry in its log with that term:
//
//	it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
//
// If it does not find an entry with that term:
//
//	it should set nextIndex = conflictIndex.
func (rf *Raft) FindNextIndex(ConflictIndex, ConflictTerm int) (next int) {
	defer DPrintf("[%d %d %v] Find nextindex = %d", rf.me, rf.CurrentTerm, rf.raftState, next)
	BaseIndex := rf.GetBaseLog().Index
	if ConflictTerm == 0 || ConflictIndex < BaseIndex || rf.Entry[ConflictIndex-BaseIndex].Term == ConflictTerm {
		next = ConflictIndex
		return
	}

	left, right := BaseIndex, rf.Entry[len(rf.Entry)-1].Index
	for left < right-1 {
		mid := left + (right-left)/2
		if rf.Entry[mid-BaseIndex].Term <= ConflictTerm {
			left = mid
		} else {
			right = mid - 1
		}
	}

	if rf.Entry[right-BaseIndex].Term == ConflictTerm {
		next = right + 1
	} else if rf.Entry[left-BaseIndex].Term == ConflictTerm {
		next = left + 1
	} else {
		next = ConflictIndex
	}

	return
}

// If there exists an N such that N > commitIndex,
// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
//
//	set commitIndex = N
func (rf *Raft) FindN() {
	// locked
	matchIndexSet := make([]int, 0)
	BaseIndex := rf.GetBaseLog().Index
	for peer := range rf.peers {
		if peer != rf.me {
			matchindex, _ := rf.GetMatchNextIndex(peer)
			matchIndexSet = append(matchIndexSet, matchindex)
		}
	}
	DPrintf("[%d %d %v] matchIndexSet: %v", rf.me, rf.CurrentTerm, rf.raftState, matchIndexSet)
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
	// locked
	DPrintf("[%d %d %v] update commitIndex %d -> %d", rf.me, rf.CurrentTerm, rf.raftState, rf.commitIndex, commitIndex)
	rf.commitIndex = commitIndex
	go func() { rf.applyCond.Signal() }()
}

func (rf *Raft) GetRequestVoteArg() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastLog().Index,
		LastLogTerm:  rf.GetLastLog().Term,
	}
	return args
}

func (rf *Raft) GetAppendEntriesArg(prevLogIndex int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.Entry[prevLogIndex-rf.GetBaseLog().Index].Term,
		LeaderCommit: rf.commitIndex,
	}

	args.Entry = append(args.Entry, rf.Entry[prevLogIndex+1-rf.GetBaseLog().Index:]...)

	return args
}

func (rf *Raft) GetSnapshotArg() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LastIncludedIndex: rf.GetBaseLog().Index,
		LastIncludedTerm:  rf.GetBaseLog().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

func (rf *Raft) ChangeState(targetState RaftState) {
	rf.raftState = targetState
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
	Err           ReplyErr
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
	Err   ReplyErr
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
	Err  ReplyErr
}

func (rf *Raft) Applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.applyCond.Wait()
		}
		CommitIndex := rf.commitIndex
		BaseIndex := rf.GetBaseLog().Index
		Entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		copy(Entries, rf.Entry[rf.lastApplied+1-BaseIndex:rf.commitIndex+1-BaseIndex])
		DPrintf("[%d %d %v] apply Entry: %v		 len(entry): %d baseindex: %d lastapplied: %d commit: %d", rf.me, rf.CurrentTerm, rf.raftState, Entries, len(rf.Entry), BaseIndex, rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()

		for _, Entry := range Entries {
			rf.applyChannel <- ApplyMsg{
				CommandValid: true,
				Command:      Entry.Command,
				CommandTerm:  Entry.Term,
				CommandIndex: Entry.Index,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, CommitIndex)
		rf.mu.Unlock()
	}
}

// PART: Election
func (rf *Raft) StartElection() {

	voteCnt := 1
	args := rf.GetRequestVoteArg()
	DPrintf("[%d %d %v] Start Election", rf.me, rf.CurrentTerm, rf.raftState)

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				// for {
				reply := &RequestVoteReply{}
				if rf.peers[peer].Call("Raft.RequestVote", args, reply) {
					rf.mu.Lock()
					if rf.raftState == Candidate && rf.CurrentTerm == args.Term {
						DPrintf("[%d %d %v] send %+v to %d and Grant %v for Err %v", rf.me, rf.CurrentTerm, rf.raftState, args, peer, reply.Grant, reply.Err)
						if reply.Grant {
							voteCnt++
							if voteCnt > len(rf.peers)/2 {
								rf.ChangeState(Leader)
								for peer := range rf.peers {
									rf.UpdateMatchNextIndex(peer, 0, rf.Entry[len(rf.Entry)-1].Index+1)
								}
								DPrintf("[%d %d %v] Leader now", rf.me, rf.CurrentTerm, rf.raftState)
								rf.StartHeartbeat(true)
								rf.ResetTimer(true)
							}
						} else if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.VoteFor = null
							rf.ChangeState(Follower)
							rf.Persist()
						}
					}
					rf.mu.Unlock()
					// return
				}
				// }
			}(i)
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	if args.Term < rf.CurrentTerm {
		reply.Term, reply.Grant, reply.Err = rf.CurrentTerm, false, ErrOldTerm
		return
	}

	if args.Term == rf.CurrentTerm && (rf.VoteFor != null && rf.VoteFor != args.CandidateId) {
		reply.Term, reply.Grant, reply.Err = rf.CurrentTerm, false, ErrVoted
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.VoteFor, rf.CurrentTerm = null, args.Term
		rf.ChangeState(Follower)
	}

	LastLogTerm := rf.GetLastLog().Term
	LastLogIndex := rf.GetLastLog().Index

	if args.LastLogTerm < LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogIndex < LastLogIndex) {
		reply.Term, reply.Grant, reply.Err = rf.CurrentTerm, false, ErrLogNotMatch
		return
	}

	reply.Term, reply.Grant, reply.Err = rf.CurrentTerm, true, OK
	rf.VoteFor = args.CandidateId
	rf.ResetTimer(false)
}

// PART: Heartbeat
func (rf *Raft) StartHeartbeat(urgent bool) {
	for peer := range rf.peers {
		if peer != rf.me {
			// if urgent {
			go rf.SendHeartBeat(peer)
			// } else {
			// 	rf.replicatorCond[peer].Broadcast()
			// }
		}
	}

}

func (rf *Raft) SendHeartBeat(peer int) {
	rf.mu.RLock()

	if rf.raftState != Leader {
		rf.mu.RUnlock()
		return
	}

	_, nextindex := rf.GetMatchNextIndex(peer)
	prevLogIndex := nextindex - 1
	BaseIndex := rf.GetBaseLog().Index

	if prevLogIndex < BaseIndex {
		SnapshotArgs := rf.GetSnapshotArg()
		rf.mu.RUnlock()
		SnapshotReply := &InstallSnapshotReply{}

		if rf.peers[peer].Call("Raft.InstallSnapshot", SnapshotArgs, SnapshotReply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.raftState == Leader && rf.CurrentTerm == SnapshotArgs.Term {
				if SnapshotReply.Term > rf.CurrentTerm {
					DPrintf("[%d %d %v] change term %d to %d", rf.me, rf.CurrentTerm, rf.raftState, rf.CurrentTerm, SnapshotReply.Term)
					rf.CurrentTerm = SnapshotReply.Term
					rf.VoteFor = null
					rf.ChangeState(Follower)
					rf.Persist()
					return
				}
				rf.UpdateMatchNextIndex(peer, BaseIndex, BaseIndex+1)
			}
		}
	} else {
		AppendEntriesArgs := rf.GetAppendEntriesArg(prevLogIndex)
		rf.mu.RUnlock()
		AppendEntriesReply := &AppendEntriesReply{}

		if rf.peers[peer].Call("Raft.AppendEntries", AppendEntriesArgs, AppendEntriesReply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("[%d %d %v] sendAppendEntries to %d and get %+v", rf.me, rf.CurrentTerm, rf.raftState, peer, AppendEntriesReply)
			if rf.raftState == Leader && rf.CurrentTerm == AppendEntriesArgs.Term {
				if AppendEntriesReply.Term > rf.CurrentTerm {
					DPrintf("[%d %d %v] change term %d to %d", rf.me, rf.CurrentTerm, rf.raftState, rf.CurrentTerm, AppendEntriesReply.Term)
					rf.VoteFor = null
					rf.CurrentTerm = AppendEntriesReply.Term
					rf.ChangeState(Follower)
					rf.Persist()
					return
				}

				if AppendEntriesReply.Success {
					rf.UpdateMatchNextIndex(peer, AppendEntriesReply.ConflictIndex, AppendEntriesReply.ConflictIndex+1)
					rf.FindN()
					return
				}
				// reply false
				next := rf.FindNextIndex(AppendEntriesReply.ConflictIndex, AppendEntriesReply.ConflictTerm)
				rf.UpdateMatchNextIndex(peer, null, next)
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	defer DPrintf("[%d %d %v] handle AppendEntriesRPC {Term: %d, PrevLogIndex: %d, PrevLogTerm: %d} and reply %+v now First Entry: %+v Last Entry: %+v", rf.me, rf.CurrentTerm, rf.raftState, args.Term, args.PrevLogIndex, args.PrevLogTerm, reply, rf.Entry[0], rf.Entry[len(rf.Entry)-1])

	if args.Term < rf.CurrentTerm {
		reply.Term, reply.Success, reply.Err = rf.CurrentTerm, false, ErrOldTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm, rf.VoteFor = args.Term, null
	}

	rf.ChangeState(Follower)
	rf.ResetTimer(false)

	BaseIndex := rf.GetBaseLog().Index

	if args.PrevLogIndex < BaseIndex {
		reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm, reply.Err = rf.CurrentTerm, false, BaseIndex, rf.Entry[0].Term, ErrPrevLogIndexUnderFlow
		return
	}

	reply.Term = rf.CurrentTerm
	// If a follower does not have prevLogIndex in its log:
	// 		it should return with conflictIndex = len(log) and conflictTerm = None.
	if args.PrevLogIndex-BaseIndex >= len(rf.Entry) {
		reply.Success, reply.ConflictIndex, reply.ConflictTerm, reply.Err = false, len(rf.Entry)+BaseIndex, 0, ErrPrevLogIndexOverFlow
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
		reply.Err = ErrLogNotMatch
		return
	}

	// prevlog matches
	reply.ConflictIndex, reply.Success, reply.Err = args.PrevLogIndex, true, OK

	if len(args.Entry) != 0 {
		idx := 0
		for _, Ent := range args.Entry {
			if Ent.Index > rf.Entry[len(rf.Entry)-1].Index {
				break
			}
			if rf.Entry[Ent.Index-BaseIndex].Term != Ent.Term {
				rf.Entry = rf.Entry[:Ent.Index-BaseIndex]
				break
			}
			idx++
		}
		for idx < len(args.Entry) {
			rf.Entry = append(rf.Entry, args.Entry[idx])
			idx++
		}
		reply.ConflictIndex = args.Entry[len(args.Entry)-1].Index
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.UpdateCommitAndApply(min(args.LeaderCommit, rf.Entry[len(rf.Entry)-1].Index))
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	defer DPrintf("[%d %d %v] handle InstallSnapshotRPC {LastIncludedIndex: %d, LastIncludedTerm: %d} and now First Entry: %+v Last Entry: %+v", rf.me, rf.CurrentTerm, rf.raftState, args.LastIncludedIndex, args.LastIncludedTerm, rf.Entry[0], rf.Entry[len(rf.Entry)-1])

	if rf.CurrentTerm > args.Term {
		reply.Term, reply.Err = rf.CurrentTerm, ErrOldTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm, rf.VoteFor = args.Term, null
	}

	rf.ChangeState(Follower)
	rf.ResetTimer(false)

	if args.LastIncludedIndex <= rf.commitIndex {
		reply.Term, reply.Err = rf.CurrentTerm, ErrOldSnapshot
		return
	}

	BaseIndex := rf.GetBaseLog().Index
	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if args.LastIncludedIndex-BaseIndex < len(rf.Entry) {
		rf.Entry = rf.Entry[args.LastIncludedIndex-BaseIndex:]
		rf.Entry[0].Index = args.LastIncludedIndex
		rf.Entry[0].Term = args.LastIncludedTerm
	} else {
		rf.Entry = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	}

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	reply.Term, reply.Err = rf.CurrentTerm, OK
	rf.persister.SaveSnapshot(args.Data)

	DPrintf("[%d %d %v] baseindex: %d lastallplied %d commitindex %d after installsnapshot", rf.me, rf.CurrentTerm, rf.raftState, BaseIndex, rf.lastApplied, rf.commitIndex)

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

	BaseIndex := rf.GetBaseLog().Index

	if index <= BaseIndex {
		DPrintf("[%d %d %v] Snapshot fail\n", rf.me, rf.CurrentTerm, rf.raftState)
		return
	}

	DPrintf("[%d %d %v] len(entry): %d baseindex: %d lastapplied: %d commit: %d", rf.me, rf.CurrentTerm, rf.raftState, len(rf.Entry), BaseIndex, rf.lastApplied, rf.commitIndex)
	rf.Entry = rf.Entry[index-BaseIndex:]
	rf.Persist()
	rf.persister.SaveSnapshot(snapshot)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isleader := (rf.raftState == Leader)

	if isleader {
		index = rf.GetLastLog().Index + 1
		term = rf.CurrentTerm
		Log := LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		rf.Entry = append(rf.Entry, Log)
		go rf.StartHeartbeat(false)
		rf.Persist()
		DPrintf("[%d %d %v] START %+v", rf.me, rf.CurrentTerm, rf.raftState, Log)
	}

	return index, term, isleader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("[%d %d %v] dead", rf.me, rf.CurrentTerm, rf.raftState)
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
				rf.StartElection()
			}
			rf.ResetTimer(false)
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.raftState == Leader {
				rf.StartHeartbeat(true)
				rf.ResetTimer(true)
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
		dead:      0,

		CurrentTerm: 0,
		VoteFor:     null,
		Entry:       []LogEntry{{0, 0, 0}},
		raftState:   Follower,

		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),

		applyChannel: applyCh,

		electionTimer:  time.NewTimer(0),
		heartbeatTimer: time.NewTimer(0),
	}

	rf.ReadPersist(persister.ReadRaftState())
	rf.ResetTimer(false)
	rf.applyCond = sync.NewCond(&rf.mu)

	for peer := range rf.peers {
		rf.UpdateMatchNextIndex(peer, 0, rf.Entry[len(rf.Entry)-1].Index+1)
	}
	DPrintf("[%d %d %v] make success", rf.me, rf.CurrentTerm, rf.raftState)
	go rf.Ticker()
	go rf.Applier()

	return rf
}
