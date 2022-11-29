package raft

import (
	"math/rand"
	"sort"
	"time"
)

func (rf *Raft) HeartBeatTimeOut() time.Duration {
	return 100 * time.Millisecond
}

func (rf *Raft) ElectionTimeOut() time.Duration {
	rand.Seed(time.Now().Unix() + int64(rf.me))
	return time.Duration(rand.Intn(450)+250) * time.Millisecond
}

func (rf *Raft) ResetTimer(isleader bool) {
	if isleader {
		rf.heartbeatTimer.Reset(rf.HeartBeatTimeOut())
	} else {
		rf.electionTimer.Reset(rf.ElectionTimeOut())
	}
}

func (rf *Raft) HasLogAtCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.GetLastLog().Term == rf.CurrentTerm
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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
		DPrintf("[%d %d %d %v] nextIndex[%d] %d -> %d", rf.GroupId, rf.me, rf.CurrentTerm, rf.raftState, peer, rf.nextIndex[peer], next)
		rf.nextIndex[peer] = next
	}
}

func (rf *Raft) FindNextIndex(ConflictIndex, ConflictTerm int) (next int) {
	// defer DPrintf("[%d %d %d %v] Find nextindex = %d", rf.GroupId, rf.me, rf.CurrentTerm, rf.raftState, next)
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
	DPrintf("[%d %d %d %v] matchIndexSet: %v", rf.GroupId, rf.me, rf.CurrentTerm, rf.raftState, matchIndexSet)
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
	DPrintf("[%d %d %d %v] update commitIndex %d -> %d", rf.GroupId, rf.me, rf.CurrentTerm, rf.raftState, rf.commitIndex, commitIndex)
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
