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
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"os"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"
const (
	DEBUG = 0
	INFO  = 1
	WARN  = 2
	ERROR = 3
)

var logLevel int

func Logf(level int, fmtstr string, args ...interface{}) {
	if level >= logLevel {
		fmt.Printf(fmtstr, args...)
	}
}

func init() {
	logLevel = WARN
	if val, ok := os.LookupEnv("LOG_LEVEL"); ok {
		switch val {
		case "DEBUG":
			logLevel = DEBUG
		case "INFO":
			logLevel = INFO
		case "WARN":
			logLevel = WARN
		case "ERROR":
			logLevel = ERROR
		}
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type VotedForType struct {
	voted bool
	Id    int
}

const (
	STATE_FOLLOWER  = iota
	STATE_CANDIDATE // also known as candidate state
	STATE_LEADER
)

const RaftHeartBeatPeriod = 50 * time.Millisecond

const RaftElectionTimeoutMin = 150 // Millisecond, inclusive
const RaftElectionTimeoutMax = 300 // exclusive

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	CurrentTerm int
	VotedFor    VotedForType
	Log         []LogEntry

	// Volatile states
	commitIndex int
	lastApplied int

	// leader-only states
	nextIndex  []int
	matchIndex []int

	// follower-only states
	lastHeartBeat time.Time

	// other stuff
	state           int
	candPeerVote    []bool // TODO this is redundant now that I only use one goroutine per peer
	candVotes       int
	candRefusals    int
	electionTimeout time.Duration
	applyCh         chan ApplyMsg
	peerSynced      []bool // eligible for scheduling
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.state == STATE_LEADER
	Logf(DEBUG, "%v get state term=%v isleader=%v\n", rf.me, term, isleader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// Logf(INFO, "%v.persist() -> Term=%v VotedFor=%v Log=%v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log)
	// Logf(INFO, "Data = %v\n", data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	// Logf(INFO, "%v.readPersist() -> Term=%v VotedFor=%v Log=%v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log)
	// Logf(INFO, "Data = %v\n", data)
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	refusal := AppendEntriesReply{
		Term:    rf.CurrentTerm,
		Success: false,
	}

	grant := AppendEntriesReply{
		Term:    rf.CurrentTerm,
		Success: true,
	}

	if args.Term < rf.CurrentTerm {
		Logf(DEBUG, "%v REFUSES stale append from %v (arg=%v, cur=%v)\n", rf.me, args.LeaderId, args.Term, rf.CurrentTerm)
		*reply = refusal
		return
	}

	rf.updateTerm(args.Term)
	rf.lastHeartBeat = time.Now()

	// Append log
	// This is skipped by heart beat and LeaderCommit update entries
	if !(len(rf.Log) > args.PrevLogIndex && (args.PrevLogIndex == -1 || rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm)) {
		// Leader needs to pick an earlier index
		*reply = refusal
		return
	}

	for i, entry := range args.Entries {
		index := i + args.PrevLogIndex + 1
		if index >= len(rf.Log) {
			rf.Log = append(rf.Log, entry)
		} else if entry.Term != rf.Log[index].Term {
			rf.Log[index] = entry
			rf.Log = rf.Log[0 : index+1]
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := len(rf.Log) - 1
		if lastLogIndex < args.LeaderCommit {
			rf.updateCommitIndex(lastLogIndex)
		} else {
			rf.updateCommitIndex(args.LeaderCommit)
		}
	}
	*reply = grant
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(reply.Term)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)

	refusal := RequestVoteReply{
		Term:        rf.CurrentTerm,
		VoteGranted: false,
	}
	grant := RequestVoteReply{
		Term:        rf.CurrentTerm,
		VoteGranted: true,
	}

	// Stale requests
	if args.Term < rf.CurrentTerm {
		Logf(DEBUG, "%v refuses stale requests from %v\n", rf.me, args.CandidateId)
		*reply = refusal
		return
	}

	// Note on followers
	// They will NOT vote for anybody else once they have voted
	// They will only transit to voting mode when not receiving heartbeats

	// A new candidate is valid if it has a more up to date log
	// 1. The log is of a newer term
	// 2. The log is of the same term, but has more entries
	grantVote := false
	if rf.VotedFor.voted == false {
		lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
		if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// This *might* be redundant
			// The paper mentions that valid requests from candidates can
			// keep the follower in follower mode, I'm guessing this is
			// what they mean by a valid request
			rf.lastHeartBeat = time.Now()
			grantVote = true
		} else {
			Logf(INFO, "%v refuses vote from %v (log check)\n", rf.me, args.CandidateId)
			Logf(INFO, "\targs.LastLogTerm=%v lastLogTerm=%v\n", args.LastLogTerm, lastLogTerm)
			Logf(INFO, "\targs.LastLogIndex=%v lastLogIndex=%v\n", args.LastLogTerm, lastLogIndex)
		}
	} else if rf.VotedFor.Id == args.CandidateId {
		grantVote = true
	}

	if grantVote {
		rf.VotedFor.voted = true
		rf.VotedFor.Id = args.CandidateId
		rf.persist()

		*reply = grant
		return
	}
	Logf(DEBUG, "%v REFUSES vote from %v\n", rf.me, args.CandidateId)
	*reply = refusal
	return
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	index = len(rf.Log)
	term = rf.CurrentTerm
	isLeader = rf.state == STATE_LEADER

	if isLeader {
		rf.LeaderAppendLog(LogEntry{
			Command: command,
			Term:    rf.CurrentTerm,
		})
	}

	return index, term, isLeader
}

func (rf *Raft) LeaderAppendLog(entry LogEntry) {
	rf.scheduleUpdatePeerLog()
	rf.Log = append(rf.Log, entry)
	rf.persist()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) genElectionTimeout() time.Duration {
	r := RaftElectionTimeoutMax - RaftElectionTimeoutMin
	rf.electionTimeout = time.Duration(RaftElectionTimeoutMin+rand.Int()%r) * time.Millisecond
	return rf.electionTimeout
}

func (rf *Raft) becomeCandidate() {
	Logf(INFO, "%v -> CANDIDATE\n", rf.me)
	rf.state = STATE_CANDIDATE

	rf.CurrentTerm += 1
	rf.VotedFor.voted = true
	rf.VotedFor.Id = rf.me
	rf.persist()

	Logf(DEBUG, "%v increases term to %v\n", rf.me, rf.CurrentTerm)

	// Volatile states
	rf.candPeerVote = make([]bool, len(rf.peers))
	rf.candPeerVote[rf.me] = true // just for the heck of it
	rf.candVotes = 1
	rf.candRefusals = 0
	rf.genElectionTimeout()

	go func(term int) {
		rf.mu.Lock()
		timeout := rf.electionTimeout
		rf.mu.Unlock()

		time.Sleep(timeout)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !(rf.state == STATE_CANDIDATE && rf.CurrentTerm == term) {
			return
		}
		if rf.candRefusals > len(rf.peers)/2 {
			Logf(INFO, "%v is unable to become leader grants=%v refusals=%v\n", rf.me, rf.candVotes, rf.candRefusals)
			return
		}
		// Restart candidacy
		Logf(INFO, "%v candidacy timeout grants=%v refusals=%v\n", rf.me, rf.candVotes, rf.candRefusals)
		rf.becomeCandidate()
	}(rf.CurrentTerm)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVoteAndUpdate(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	Logf(INFO, "%v -> LEADER\n", rf.me)
	rf.state = STATE_LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.peerSynced = make([]bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.peerSynced[i] = true
		if i != rf.me {
			rf.nextIndex[i] = len(rf.Log)
			rf.matchIndex[i] = 0
		}
	}
	rf.scheduleUpdatePeerLog()
	rf.ScheduleTick()
}

func (rf *Raft) becomeFollower() {
	Logf(INFO, "%v -> FOLLOWER\n", rf.me)
	rf.VotedFor.voted = false
	rf.persist() // TODO srsly
	rf.state = STATE_FOLLOWER
	rf.genElectionTimeout()
	rf.lastHeartBeat = time.Now() // dunno about this

	rf.ScheduleTick()
}

func (rf *Raft) lastLogIndexTerm() (int, int) {
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.Log[lastLogIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) requestVoteAndUpdate(peerIndex int) {
	var reply RequestVoteReply

	rf.mu.Lock()

	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	curTerm := rf.CurrentTerm

	rf.mu.Unlock()

	ok := rf.sendRequestVote(peerIndex, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !(curTerm == rf.CurrentTerm && rf.state == STATE_CANDIDATE) {
		return
	}

	if !ok {
		go func() {
			time.Sleep(RaftHeartBeatPeriod)
			rf.requestVoteAndUpdate(peerIndex)
		}()
		return
	}

	rf.updateTerm(reply.Term)
	if rf.state != STATE_CANDIDATE {
		return
	}

	if reply.VoteGranted {
		if !rf.candPeerVote[peerIndex] {
			rf.candVotes += 1
			rf.candPeerVote[peerIndex] = true
			if rf.candVotes > len(rf.candPeerVote)/2 {
				rf.becomeLeader()
				return
			}
		}
	} else {
		rf.candRefusals += 1
	}
}

func (rf *Raft) updatePeerLog(routineTerm int, peerIndex int, retries int) {
	// This method recursive:
	// updatePeerLog -> reply (success) -> leader updates -> updatePeerLog ...
	//               \-> reply (fail) -> heartbeat period -> updatePeerLog

	// To avoid goroutine leak when state changes from
	// Leader (old term) -> Follower -> Leader (new term)
	// we need to invalidate this if our term doesn't match anymore

	Logf(DEBUG, "%v waking up\n", peerIndex)
	defer Logf(DEBUG, "%v sleeping\n", peerIndex)

	rf.mu.Lock()

	if !(rf.CurrentTerm == routineTerm && rf.state == STATE_LEADER) {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := -1
	prevLogTerm := 0

	// Send empty log when we need to update LeaderCommit
	entries := []LogEntry{}
	if rf.nextIndex[peerIndex] < len(rf.Log) {
		entries = rf.Log[rf.nextIndex[peerIndex]:]
	}
	if rf.nextIndex[peerIndex] > 0 {
		prevLogIndex = rf.nextIndex[peerIndex] - 1
		prevLogTerm = rf.Log[prevLogIndex].Term
	}

	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply

	// The way this function is written is vulnerable to arbitrarily long
	// network latency, hence the timer. We need to ensure that nodes
	// exchange messages at a rate approximately the heartbeat period

	doneCh := make(chan bool)
	expireCh := make(chan int)

	var ok bool
	go func() {
		doneCh <- rf.sendAppendEntries(peerIndex, &args, &reply)
	}()

	go func() {
		time.Sleep(RaftHeartBeatPeriod)
		expireCh <- 1
	}()

	select {
	case ok = <-doneCh:
		go func() { <-expireCh }()
	case _ = <-expireCh:
		go func() { <-doneCh }()
		ok = false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.me

	// updateTerm demoted us or this goroutine is stale
	if !(rf.CurrentTerm == routineTerm && rf.state == STATE_LEADER) {
		return
	}

	if !ok {
		// Retry at heartbeat interval
		duration := RaftHeartBeatPeriod * time.Duration(retries)
		Logf(INFO, "%v will retry %v after %v\n", me, peerIndex, duration)
		go func() {
			time.Sleep(duration)
			if retries < 32 {
				retries *= 2
			}
			rf.updatePeerLog(routineTerm, peerIndex, retries)
		}()
		return
	}

	if !reply.Success {
		// Retry with smaller nextIndex
		rf.nextIndex[peerIndex] -= 1

		if rf.nextIndex[peerIndex] < 0 {
			panic(fmt.Sprintf("%v's nextIndex for %v is negative", rf.me, peerIndex))
		}
		go rf.updatePeerLog(routineTerm, peerIndex, 1)
		return
	}

	rf.nextIndex[peerIndex] += len(args.Entries)
	rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1

	// commit index changed, notify all peers
	changed := rf.findNewCommitIndex()
	if changed {
		rf.peerSynced[peerIndex] = true // schedule this too
		rf.scheduleUpdatePeerLog()
		return
	}

	// only notify this peer
	if rf.nextIndex[peerIndex] < len(rf.Log) || rf.commitIndex > args.LeaderCommit {
		go rf.updatePeerLog(routineTerm, peerIndex, 1)
		return
	}
	rf.peerSynced[peerIndex] = true
}

func (rf *Raft) scheduleUpdatePeerLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.peerSynced[i] {
			rf.peerSynced[i] = false
			go rf.updatePeerLog(rf.CurrentTerm, i, 1)
		}
	}
}

func (rf *Raft) majorityPersisted(index int) bool {
	nPersisted := 1
	for i := 0; i < len(rf.matchIndex); i++ {
		if i != rf.me && rf.matchIndex[i] >= index {
			nPersisted += 1
			if nPersisted > len(rf.peers)/2 {
				return true
			}
		}
	}
	return false
}

func (rf *Raft) findNewCommitIndex() bool {
	changed := false
	for i := rf.commitIndex + 1; i < len(rf.Log); i++ {
		if rf.majorityPersisted(i) {
			Logf(INFO, "%v.commits(%v)\n", rf.me, i)
			rf.updateCommitIndex(i)
			changed = true
		} else {
			break
		}
	}
	return changed
}

func (rf *Raft) updateCommitIndex(index int) {
	oldIndex := rf.commitIndex
	Logf(INFO, "%v.updateCommitIndex(%v -> %v: %v)\n", rf.me, oldIndex+1, index, rf.Log[oldIndex+1:index+1])
	rf.commitIndex = index
	for i := oldIndex + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			Index:   i,
			Command: rf.Log[i].Command,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) _tick() {
	switch rf.state {
	case STATE_FOLLOWER:
		{
			now := time.Now()
			if now.Sub(rf.lastHeartBeat) > rf.electionTimeout {
				rf.becomeCandidate()
				return
			}
		}
	case STATE_CANDIDATE:
		// do nothing
	case STATE_LEADER:
		// acts as heart beat
		rf.scheduleUpdatePeerLog()
	default:
		panic("Shouldn't happen")
	}
	rf.ScheduleTick()
}

func (rf *Raft) ScheduleTick() {
	go func() {
		time.Sleep(RaftHeartBeatPeriod)
		rf.Tick()
	}()
}

func (rf *Raft) Tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf._tick()
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.CurrentTerm {
		Logf(DEBUG, "%v updates term from %v to %v\n", rf.me, rf.CurrentTerm, term)
		rf.CurrentTerm = term
		rf.persist()
		rf.becomeFollower()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	rf.applyCh = applyCh
	rf.becomeFollower()

	return rf
}
