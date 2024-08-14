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

import (
	"bytes"
	"context"
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index   int
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.RWMutex    // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	state           string
	applyCh         chan ApplyMsg
	lastLeaderCheck time.Time

	currentTerm       int
	votedFor          int
	logs              []Entry
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	lastIncludedTerm  int
	lastIncludedIndex int
	applyCond         *sync.Cond

	elementVoteCount int
	waitingSnapshot  []byte
	waitingIndex     int
	waitingTerm      int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == "Leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		logs              []Entry
		currentTerm       int
		votedFor          int
		commitIndex       int
		lastIncludedIndex int
	)
	if d.Decode(&logs) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&commitIndex) != nil || d.Decode(&lastIncludedIndex) != nil {
		DPrintf("error")
	} else {
		rf.logs = logs
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastIncludedIndex = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	lastIncludeIndex := rf.lastIncludedIndex
	if lastIncludeIndex == -1 {
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = rf.logs[index].Term
		rf.logs = rf.logs[index+1:]
	} else if index > lastIncludeIndex {
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = rf.logs[index-lastIncludeIndex-1].Term
		rf.logs = rf.logs[index-lastIncludeIndex:]
	}
	rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SendInstallSnapshot(i int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludedIndex,
		LastIncludeTerm:  rf.lastIncludedTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if rf.peers[i].Call("Raft.InstallSnapshot", args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != "Leader" {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = "Follower"
			rf.persist()
			return
		}
		newNext := args.LastIncludeIndex + 1
		newMatch := args.LastIncludeIndex

		if newNext > rf.nextIndex[i] {
			rf.nextIndex[i] = newNext
		}
		if newMatch > rf.matchIndex[i] {
			rf.matchIndex[i] = newMatch
		}

	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
	}

	if rf.commitIndex > args.LastIncludeIndex {
		rf.persist()
		return
	}

	if rf.waitingIndex < args.LastIncludeIndex {
		rf.waitingSnapshot = args.Data
		rf.waitingTerm = args.LastIncludeTerm
		rf.waitingIndex = args.LastIncludeIndex
	}

	rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	rf.lastApplied = rf.commitIndex

	if rf.commitIndex > rf.GetLogsLen() {
		rf.logs = make([]Entry, 0)
	} else {
		if rf.lastIncludedIndex == -1 {
			rf.logs = rf.logs[args.LastIncludeIndex-rf.lastIncludedIndex+1:]
		} else {
			rf.logs = rf.logs[args.LastIncludeIndex-rf.lastIncludedIndex:]
		}
	}

	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludeTerm

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)
	rf.applyCond.Broadcast()
}

//func (rf *Raft) applySnapshot(snapshot []byte) {
//	snap := ApplyMsg{SnapshotValid: true,
//		Snapshot:      snapshot,
//		SnapshotTerm:  rf.lastIncludedTerm,
//		SnapshotIndex: rf.lastIncludedIndex}
//	rf.applyCh <- snap
//}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	CommitIndex  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	VoteGranted bool
	Term        int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.CommitIndex < rf.commitIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
	}

	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastLeaderCheck = time.Now()
	} else {
		reply.VoteGranted = false
	}

	rf.persist()
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "Leader" {
		return -1, -1, false
	}

	index := len(rf.logs)
	term := rf.currentTerm
	rf.logs = append(rf.logs, Entry{Term: term, Command: command, Index: index})
	rf.persist()
	return index, term, rf.state == "Leader"
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) initLeaderState() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		if rf.lastIncludedIndex == -1 {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = len(rf.logs) - 1
		} else {
			rf.nextIndex[i] = rf.lastIncludedIndex + 1
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
	}
}

func (rf *Raft) startElement() {
	rf.mu.Lock()
	rf.state = "Candidate"
	rf.currentTerm++
	rf.elementVoteCount = 1
	rf.votedFor = rf.me
	rf.lastLeaderCheck = time.Now()
	done := make(chan struct{})
	rf.persist()
	rf.mu.Unlock()

	var wg sync.WaitGroup

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		CommitIndex: rf.commitIndex,
	}
	if len(rf.logs) > 0 {
		args.LastLogIndex = len(rf.logs) - 1
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range rf.peers {
		if rf.state != "Candidate" {
			return
		}
		if i != rf.me {
			wg.Add(1)
			go func(i int, args *RequestVoteArgs) {
				defer wg.Done()

				select {
				case <-ctx.Done():
					return
				default:
				}

				if rf.state != "Candidate" {
					return
				}

				reply := &RequestVoteReply{}
				if rf.sendRequestVote(i, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.state = "Follower"
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.elementVoteCount = 0
						cancel()
					} else if reply.VoteGranted {
						rf.elementVoteCount++
						if rf.elementVoteCount > (len(rf.peers)/2) && rf.state == "Candidate" {
							rf.state = "Leader"
							rf.persist()
							rf.initLeaderState()
							go rf.sendAppendEntries()
							cancel()
						}
					}
				}

			}(i, args)
		}
	}

	timer := time.NewTimer(250 * time.Millisecond)

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		cancel()
	case <-timer.C:
		cancel()
	}
	if !timer.Stop() {
		<-timer.C
	}
}

func (rf *Raft) ticker() {
	rd := rand.New(rand.NewSource(int64(rf.me + time.Now().Second())))
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		ms := rd.Int63n(100)
		if time.Since(rf.lastLeaderCheck) > time.Duration(300+ms)*time.Millisecond {
			rf.startElement()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms = 50 + (rd.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastLeaderCheck = time.Now()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
	}

	if args.PrevLogIndex > len(rf.logs)-1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		return
	}

	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Logs...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.applyCond.Broadcast()
	rf.persist()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {

		if rf.waitingSnapshot != nil {
			msg := ApplyMsg{SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotIndex: rf.waitingIndex,
				SnapshotTerm:  rf.waitingTerm,
			}
			rf.waitingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
			if rf.lastIncludedIndex == -1 {
				msg.Command = rf.logs[rf.lastApplied].Command
			} else {
				msg.Command = rf.logs[rf.lastApplied-rf.lastIncludedIndex-1].Command
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}

	}

}

func (rf *Raft) updateCommitIndex() {

	for n := len(rf.logs) - 1; n > rf.commitIndex; n-- {

		if rf.logs[n].Term != rf.currentTerm {
			break
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			break
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	for !rf.killed() {
		rf.lastLeaderCheck = time.Now()
		if rf.state != "Leader" {
			return
		}

		t0 := time.Now()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			nextIndex := rf.nextIndex[i]
			if nextIndex <= rf.lastIncludedIndex {
				rf.mu.Unlock()
				go rf.SendInstallSnapshot(i)
				continue
			}

			logs := make([]Entry, 0)
			oldNext := rf.GetNextIndex(i)
			if oldNext < len(rf.logs) {
				logs = rf.logs[oldNext:]
			}

			var prevlogterm, prevlogindex int
			if oldNext > 0 {
				prevlogindex = oldNext - 1
				prevlogterm = rf.logs[prevlogindex].Term
			} else {
				prevlogindex = rf.lastIncludedIndex
				prevlogterm = rf.lastIncludedTerm
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevlogindex,
				PrevLogTerm:  prevlogterm,
				Logs:         logs,
			}
			rf.mu.Unlock()
			go func(i int, args *AppendEntriesArgs) {
				if rf.state != "Leader" {
					return
				}
				reply := &AppendEntriesReply{}
				if rf.state != "Leader" {
					return
				}

				if rf.peers[i].Call("Raft.AppendEntries", args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = "Follower"
						rf.votedFor = -1
						rf.persist()
						return
					}

					if rf.state != "Leader" || rf.currentTerm != args.Term {
						return
					}

					if reply.Success {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Logs)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.updateCommitIndex()
					} else {
						if reply.ConflictTerm != -1 {
							lastIndex := -1
							for i := range rf.logs {
								if rf.logs[i].Term == reply.ConflictTerm {
									lastIndex = i
								}
							}
							if lastIndex != -1 {
								rf.nextIndex[i] = lastIndex + 1
							} else {
								rf.nextIndex[i] = reply.ConflictIndex
							}
						} else {
							rf.nextIndex[i] = reply.ConflictIndex
						}
					}
				}
			}(i, args)
			time.Sleep(10 * time.Millisecond)
		}

		for time.Since(t0).Milliseconds() < 120 {
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "Follower"
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.logs = make([]Entry, 0)
	rf.logs = append(rf.logs, Entry{Term: 0})
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.lastIncludedIndex = -1
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
