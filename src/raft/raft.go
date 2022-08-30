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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VoteFor     int        // candidateId that received vote in current term (or null if none)
	Entry       []LogEntry // log[index-1] -> {term, command}
	raftState   RaftState

	commitIndex  int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastlogindex int
	lastApplied  int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	commited   []int // cnt for each entry's commited situation

	applyChannel chan ApplyMsg

	timer   int
	timeout int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	term := rf.CurrentTerm
	isleader := (rf.raftState == Leader)
	rf.mu.RUnlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Entry)
	data := w.Bytes()
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
	}

}

func (rf *Raft) resetTimer() {
	rf.timer = 0
	rand.Seed(time.Now().Unix() + int64(rf.me))
	rf.timeout = rand.Intn(150) + 350
}

// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
// If it finds an entry in its log with that term:
// 		it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
// If it does not find an entry with that term:
// 		it should set nextIndex = conflictIndex.
func (rf *Raft) findNextIndex(ConflictIndex, ConflictTerm, prevLogIndex int) int {
	if ConflictTerm == 0 || rf.Entry[ConflictIndex].Term == ConflictTerm {
		return ConflictIndex
	}
	left, right := 1, prevLogIndex

	for left < right-1 {
		mid := left + (right-left)/2
		if rf.Entry[mid].Term <= ConflictTerm {
			left = mid
		} else {
			right = mid - 1
		}
	}

	if rf.Entry[right].Term == ConflictTerm {
		return right + 1
	} else if rf.Entry[left].Term == ConflictTerm {
		return left + 1
	}

	return ConflictIndex
}

func (rf *Raft) UpdateCommitAndApply(commitIndex int) {
	rf.commitIndex = commitIndex
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyChannel <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Entry[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) findN() {
	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// 		set commitIndex = N
	matchIndexSet := make([]int, 0)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		matchIndexSet = append(matchIndexSet, rf.matchIndex[i])
	}
	sort.Ints(matchIndexSet)
	N := matchIndexSet[len(rf.peers)/2]

	if N > rf.commitIndex && rf.Entry[N].Term == rf.CurrentTerm {
		rf.UpdateCommitAndApply(N)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	rf.raftState = Leader
	// fmt.Printf("[%d] become Leader at Term : %d\n", rf.me, rf.CurrentTerm)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Entry)
		rf.matchIndex[i] = 0
	}
	rf.resetTimer()
	rf.mu.Unlock()

	go func() {
		for !rf.killed() {
			if _, isleader := rf.GetState(); isleader == false {
				return
			}
			rf.sendHeartBeat()
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (rf *Raft) BecomeCandidate() {
	// • Increment CurrentTerm
	// • Vote for self
	// • Reset election timer
	// • Send RequestVote RPCs to all other servers
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VoteFor = rf.me
	rf.resetTimer()
	rf.raftState = Candidate
	rf.persist()
	// fmt.Printf("[%d] become Candidate at Term : %d\n", rf.me, rf.CurrentTerm)
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
	totalCnt := 1

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.Entry) - 1,
			LastLogTerm:  rf.Entry[len(rf.Entry)-1].Term,
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			for {
				if rf.raftState != Candidate {
					return
				}

				ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
				if ok {
					totalCnt++
					if reply.Grant {
						mu.Lock()
						defer mu.Unlock()
						voteCnt++
						// fmt.Printf("[%d] get grant from %d\n", rf.me, server)
					} else if reply.Term > rf.CurrentTerm {
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
	for voteCnt <= len(rf.peers)/2 && totalCnt != len(rf.peers) {
		cond.Wait()
	}

	if voteCnt > len(rf.peers)/2 {
		rf.BecomeLeader()
	} else {
		rf.mu.Lock()
		rf.raftState = Follower
		rf.mu.Unlock()
	}
	mu.Unlock()
}

func (rf *Raft) sendHeartBeat() {
	// fmt.Println("[", rf.me, "]", "Term:", rf.CurrentTerm, "Entries:", rf.Entry, "last log index:", len(rf.Entry)-1)
	term, _ := rf.GetState()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}

			args.Entry = make([]LogEntry, 0)
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.Entry[args.PrevLogIndex].Term

			for idx := rf.nextIndex[server]; idx <= len(rf.Entry)-1; idx++ {
				args.Entry = append(args.Entry, rf.Entry[idx])
			}
			if _, isleader := rf.GetState(); isleader == false {
				return
			}

			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if ok {
				// fmt.Printf("%+v\n", args)
				if reply.Term > rf.CurrentTerm {
					rf.BecomeFollwer(reply.Term)
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success {
					if len(args.Entry) > 0 {
						for len(rf.commited) <= len(rf.Entry)-1 {
							rf.commited = append(rf.commited, 0)
						}
						for idx := args.PrevLogIndex + 1; idx <= len(rf.Entry)-1; idx++ {
							if rf.Entry[idx].Term != rf.CurrentTerm {
								// Raft never commits log entries from previous terms by count- ing replicas
								continue
							}
							rf.commited[idx]++
							if rf.commited[idx] == len(rf.peers)/2 { // majority
								rf.UpdateCommitAndApply(idx)
								// fmt.Println(rf.me, "commit index:", rf.commitIndex, "commited command:")
								// for j := 1; j <= rf.commitIndex; j++ {
								// 	fmt.Printf("%v ", rf.Entry[j-1].Command)
								// }
								// fmt.Printf("\n")
							}
						}
					}
					rf.matchIndex[server] = reply.ConflictIndex
					rf.nextIndex[server] = reply.ConflictIndex + 1
					rf.findN()
					return
				}
				rf.nextIndex[server] = rf.findNextIndex(reply.ConflictIndex, reply.ConflictTerm, args.PrevLogIndex)
			}

		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Grant = true
	if args.Term > rf.CurrentTerm {
		rf.BecomeFollwer(args.Term)
		// reply.Grant = true
	} else if args.Term < rf.CurrentTerm {
		// §1  Reply false if term < CurrentTerm
		// fmt.Println(rf.me, "deny", args.CandidateId, "for old term")
		reply.Term = rf.CurrentTerm
		reply.Grant = false
		return
	}
	reply.Term = rf.CurrentTerm
	// Election Restriction:
	// the RPC includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	// If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	LastLogTerm := rf.Entry[len(rf.Entry)-1].Term

	// §2  If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.VoteFor == null || rf.VoteFor == args.CandidateId {
		if args.LastLogTerm < LastLogTerm {
			// fmt.Println(rf.me, "deny", args.CandidateId, "for args.LastLogTerm < LastLogTerm")
			reply.Grant = false
		} else if args.LastLogTerm == LastLogTerm && args.LastLogIndex < len(rf.Entry)-1 {
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
		rf.resetTimer()
		// fmt.Println(rf.me, "grant", args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("[", rf.me, "]", "prevlogindex:", args.PrevLogIndex, "prevlogterm", args.PrevLogTerm)
	reply.Success = true
	reply.Term = max(args.Term, rf.CurrentTerm)
	// §1  Reply false if term < CurrentTerm
	if args.Term < rf.CurrentTerm {
		// fmt.Println("old term append rpc")
		reply.Success = false
		return
	}

	rf.BecomeFollwer(args.Term)

	// §2  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > len(rf.Entry)-1 || rf.Entry[args.PrevLogIndex].Term != args.PrevLogTerm {
		// If a follower does not have prevLogIndex in its log:
		// 		it should return with conflictIndex = len(log) and conflictTerm = None.
		// If a follower does have prevLogIndex in its log, but the term does not match:
		// 		it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
		if args.PrevLogIndex > len(rf.Entry)-1 {
			reply.ConflictIndex = len(rf.Entry)
			reply.ConflictTerm = 0
		} else if rf.Entry[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.ConflictIndex =
				func() int { // search its log for the first index whose entry has term equal to conflictTerm.
					left, right := 1, args.PrevLogIndex
					for left < right-1 {
						mid := left + (right-left)/2
						if rf.Entry[mid].Term >= rf.Entry[args.PrevLogIndex].Term {
							right = mid // right bound compact
						} else {
							left = mid + 1 // left bound compact
						}
					}
					if rf.Entry[left].Term != rf.Entry[args.PrevLogIndex].Term {
						return right
					}
					return left
				}()
			reply.ConflictTerm = rf.Entry[args.PrevLogIndex].Term
		}
		reply.Success = false
		return
	}

	reply.ConflictIndex = args.PrevLogIndex
	if len(args.Entry) != 0 { // NOT a HeartBeat
		idx := 1
		for args.PrevLogIndex+idx <= len(rf.Entry)-1 && idx <= len(args.Entry) {
			if rf.Entry[args.PrevLogIndex+idx].Term != args.Entry[idx-1].Term {
				// §3  If an existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it
				// §4  Append any new entries not already in the Entry
				rf.Entry = rf.Entry[:args.PrevLogIndex+idx]
				break
			}
			idx++
		}
		for idx <= len(args.Entry) {
			rf.Entry = append(rf.Entry, args.Entry[idx-1])
			idx++
		}
		reply.ConflictIndex = len(rf.Entry) - 1
		rf.persist()
	}

	// §5  If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	// §6  If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	if args.LeaderCommit > rf.commitIndex {
		rf.UpdateCommitAndApply(min(args.LeaderCommit, len(rf.Entry)))
	}
	// fmt.Printf("[%d] Entry: %v\n", rf.me, rf.Entry)
	rf.resetTimer()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.Entry)
	term := rf.CurrentTerm
	isLeader := (rf.raftState == Leader)

	if isLeader {
		Log := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.Entry = append(rf.Entry, Log)
		rf.persist()
	}

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond)
		if rf.raftState != Leader {
			rf.timer++
			if rf.timer == rf.timeout {
				rf.BecomeCandidate()
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
		lastlogindex: 0,
		applyChannel: applyCh,
	}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resetTimer()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.Entry[0].Term != 0 {
		rf.Entry = append([]LogEntry{{0, 0}}, rf.Entry...)
	}

	go rf.ticker()

	return rf
}
