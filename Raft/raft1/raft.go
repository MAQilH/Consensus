package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	//	"raft/labgob"
	"raft/labgob"
	"raft/labrpc"
	"raft/raftapi"
	tester "raft/tester1"
)

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const LEADER_AVAILIBILITY = 600 * time.Millisecond
const ELECTION_RANDOM_TIMEOUT = 600 * time.Millisecond
const TICKER_BASE_SLEEP_CYCLE = 50
const TICKER_RANDOM_SLEEP_CYCLE = 100

type Raft struct {
	mu        sync.Mutex
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	applyCh chan raftapi.ApplyMsg

	// Presistent state
	currentTerm int
	votedFor    int
	logs        []Log
	snapshot    []byte

	// Volatile state
	commitIndex            int
	lastApplied            int
	lastLeaderActivityTime time.Time
	electionRandomTimeout  time.Duration
	role                   Role
	syncFollowerStamp      []int
	penddingSnapshotaApply bool

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.checkIsLeaderWithoutLock()
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	state := w.Bytes()
	rf.persister.Save(state, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if len(data) < 1 {
		return
	}
	DPrintf("read persist for peer: %v", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&rf.currentTerm); err != nil {
		panic("Cann't decode current term")
	}

	if err := d.Decode(&rf.votedFor); err != nil {
		panic("Cann't decode voter for")
	}

	if err := d.Decode(&rf.logs); err != nil {
		panic("Cann't decode peer logs")
	}

	rf.snapshot = snapshot
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotRequest struct {
	Term          int
	SnapshotTerm  int
	SnapshotIndex int
	Snapshot      []byte
}

type InstallSnapshotReply struct {
	Success bool
	Term    int
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("new snapshot, index: %v", index)

	_, pos, err := rf.getLogWithIndex(index)
	if err != nil {
		DPrintf("Cant to fetch Index %v for Persist Snapshot", index)
		return
	}

	if !rf.checkIsLeaderWithoutLock() {
		DPrintf("peer %v, is not leader send snapshot to leader, snapshot-index: %v", rf.me, index)
		return
	}

	rf.snapshot = snapshot
	rf.logs = rf.logs[pos:]
	rf.persist()

	if rf.lastApplied < index {
		rf.penddingSnapshotaApply = true
		rf.cond.Broadcast()
	}

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		go rf.sendSnapshot(peerID)
	}
}

func (rf *Raft) sendSnapshot(peerID int) {
	rf.mu.Lock()
	DPrintf("Send Snapshot to peer %v", peerID)

	sIndex, sTerm := rf.getSnapshotIndexTermWithoutLock()

	args := InstallSnapshotRequest{
		Term:          rf.currentTerm,
		SnapshotTerm:  sTerm,
		SnapshotIndex: sIndex,
		Snapshot:      rf.snapshot,
	}

	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.peers[peerID].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		DPrintf("Unable InstallSnapshot; peer: %v, leader %v", peerID, rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		DPrintf("peer %v accepted snapshot with index %v from leader %v", peerID, sIndex, rf.me)
		rf.matchIndex[peerID] = max(rf.matchIndex[peerID], sIndex)
		rf.nextIndex[peerID] = max(rf.nextIndex[peerID], sIndex+1)
	} else {
		DPrintf("peer %v rejected snapshot with index %v from leader %v, current-term: %v, reply-term: %v", peerID, sIndex, rf.me, rf.currentTerm, reply.Term)
		if reply.Term > rf.currentTerm {
			rf.changeRoleToFollowerWithoutLock(reply.Term, true)
		}
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeRoleToFollowerWithoutLock(args.Term, true)
	}
	rf.lastLeaderActivityTime = time.Now()

	sLog, pos, err := rf.getLogWithIndex(args.SnapshotIndex)

	haveSLog := true
	if err != nil || sLog.Term != args.SnapshotTerm {
		haveSLog = false
	}

	if haveSLog {
		rf.logs = rf.logs[pos:]
	} else {
		rf.logs = make([]Log, 0)
		rf.logs = append(rf.logs, Log{
			Index: args.SnapshotIndex,
			Term:  args.SnapshotTerm,
		})
	}

	rf.snapshot = args.Snapshot

	rf.persist()

	if rf.lastApplied < args.SnapshotIndex {
		rf.penddingSnapshotaApply = true
		rf.cond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) getSnapshotIndexTermWithoutLock() (int, int) {
	return rf.logs[0].Index, rf.logs[0].Term
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term         int
	VotedGranted bool
}

type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type ConfilictResolution struct {
	LastIndex int
	LastTerm  int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	Confilict     ConfilictResolution
	SnapshotIndex int
}

func (rf *Raft) changeRoleToFollowerWithoutLock(term int, refreshActiveTime bool) {
	rf.role = Follower
	rf.currentTerm = term
	if refreshActiveTime {
		rf.lastLeaderActivityTime = time.Now()
		rf.electionRandomTimeout = time.Duration(rand.Int63() % int64(ELECTION_RANDOM_TIMEOUT))
	}
	rf.persist()
}

func (rf *Raft) startElection() {
	DPrintf("node %v started election", rf.me)
	electedTerm := rf.initStartElection()

	lastLogTerm, lastLogIndex := rf.GetLastTermIndexWithLock()
	votedCh := make(chan bool, len(rf.peers)-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wgSendRequestVote := sync.WaitGroup{}

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		wgSendRequestVote.Add(1)
		go rf.sendElectionToPeer(ctx, peerID, electedTerm, lastLogIndex, lastLogTerm, votedCh, &wgSendRequestVote)
	}
	wgSendRequestVote.Wait()

	if !rf.checkElectionState(electedTerm, votedCh) {
		return
	}

	rf.initLeader(lastLogIndex)
	DPrintf("New leader %v, term: %v", rf.me, electedTerm)
}

func (rf *Raft) initStartElection() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	electedTerm := rf.currentTerm + 1
	rf.currentTerm = electedTerm
	rf.votedFor = rf.me
	rf.role = Candidate

	rf.lastLeaderActivityTime = time.Now()
	rf.electionRandomTimeout = time.Duration(rand.Int63() % int64(ELECTION_RANDOM_TIMEOUT))

	rf.persist()
	return electedTerm
}

func (rf *Raft) sendElectionToPeer(ctx context.Context, peerID, electedTerm, lastLogIndex,
	lastLogTerm int, votedCh chan bool, wgSendRequestVote *sync.WaitGroup) {
	args := &RequestVoteArgs{
		Term:         electedTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	reply := &RequestVoteReply{}

	wgSendRequestVote.Done()
	ok := rf.peers[peerID].CallWithContext(ctx, "Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("Request Vote %v -> %v, term: %v failed", rf.me, peerID, electedTerm)
		votedCh <- false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.VotedGranted {
		DPrintf("%v voted -> %v, term: %v", peerID, rf.me, electedTerm)
		votedCh <- true
	} else {
		DPrintf("%v not voted -> %v, term: %v, voter term = %v", peerID, rf.me, electedTerm, reply.Term)
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.persist()
		}
		votedCh <- false
	}
}

func (rf *Raft) checkElectionState(electedTerm int, votedCh chan bool) bool {
	votedCount := 1
	majority := len(rf.peers)/2 + 1
	remind := len(rf.peers) - 1
	for range len(rf.peers) - 1 {
		remind -= 1
		if <-votedCh {
			votedCount += 1
			if votedCount >= majority {
				break
			}
		} else {
			rf.mu.Lock()
			if rf.currentTerm > electedTerm {
				votedCount = -1
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
		if votedCount+remind < majority {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	return votedCount >= majority && currentTerm <= electedTerm && rf.role == Candidate
}

func (rf *Raft) initLeader(lastLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	snapshotIndex, _ := rf.getSnapshotIndexTermWithoutLock()

	for peerId := range len(rf.peers) {
		rf.nextIndex[peerId] = lastLogIndex + 1
		rf.matchIndex[peerId] = snapshotIndex
	}

	rf.persist()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VotedGranted = false

	if args.Term <= rf.currentTerm {
		return
	}

	rf.changeRoleToFollowerWithoutLock(args.Term, false)

	lastLogTerm, lastLogIndex := rf.GetLastTermIndexWithoutLock()

	if lastLogTerm > args.LastLogTerm {
		return
	}

	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		return
	}

	rf.changeRoleToFollowerWithoutLock(args.Term, true)
	rf.votedFor = args.CandidateId
	rf.persist()

	reply.VotedGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.SnapshotIndex, _ = rf.getSnapshotIndexTermWithoutLock()

	prevLog, prevPos := rf.getLowerboundLog(args.PrevLogIndex, args.PrevLogTerm)
	reply.Confilict = ConfilictResolution{
		LastIndex: prevLog.Index,
		LastTerm:  prevLog.Term,
	}

	if rf.currentTerm != args.Term {
		if args.Term < rf.currentTerm {
			return
		}
		rf.changeRoleToFollowerWithoutLock(args.Term, true)
	}
	rf.lastLeaderActivityTime = time.Now()

	if prevLog.Index != args.PrevLogIndex {
		return
	}
	if prevLog.Term != args.PrevLogTerm {
		return
	}

	lastMatchedIndexWithLeader := args.LeaderCommit

	if len(args.Entries) > 0 {
		correctedLogs := rf.logs[:prevPos+1]
		correctedLogs = append(correctedLogs, args.Entries...)
		rf.logs = correctedLogs
		rf.persist()

		lastMatchedIndexWithLeader = min(lastMatchedIndexWithLeader, args.Entries[len(args.Entries)-1].Index)
	} else {
		lastMatchedIndexWithLeader = min(lastMatchedIndexWithLeader, args.PrevLogIndex)
	}

	if lastMatchedIndexWithLeader > rf.commitIndex {
		_, lastIndex := rf.GetLastTermIndexWithoutLock()
		rf.commitIndex = min(lastIndex, lastMatchedIndexWithLeader)
		rf.cond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) applyCommitedLogs() {
	for {
		rf.mu.Lock()
		rf.cond.Wait()
		rf.mu.Unlock()

		for {
			rf.mu.Lock()
			var msg raftapi.ApplyMsg
			
			if rf.penddingSnapshotaApply {
				index, term := rf.getSnapshotIndexTermWithoutLock()
				msg = raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  term,
					SnapshotIndex: index,
				}
				rf.penddingSnapshotaApply = false
				rf.lastApplied = max(rf.lastApplied, index)
			} else if rf.commitIndex > rf.lastApplied {
				log, _, err := rf.getLogWithIndex(rf.lastApplied + 1)
				snapshotIndex, _ := rf.getSnapshotIndexTermWithoutLock()
				if err != nil {
					panic(fmt.Errorf("Unexpected Error in commit Logs in peer %v, commit: %v, logLen: %v, lastApplied: %v, snapshot-index: %v, err: %s", rf.me, rf.commitIndex, len(rf.logs), rf.lastApplied, snapshotIndex, err))
				}
				msg = raftapi.ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: log.Index,
				}
				rf.lastApplied += 1
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) getLogWithIndex(index int) (Log, int, error) {
	if len(rf.logs) == 0 {
		return Log{}, -1, errors.New("Current Logs was been empty!")
	}

	snapshotIndex, _ := rf.getSnapshotIndexTermWithoutLock()
	if snapshotIndex > index {
		return Log{}, -1, errors.New("This index was been compacted!")
	}

	pos := index - snapshotIndex
	if pos >= len(rf.logs) {
		return Log{}, -1, errors.New("This peer doesn't have this index yet!")
	}

	return rf.logs[pos], pos, nil
}

func (rf *Raft) getLowerboundLog(index, term int) (Log, int) {
	lx := 0
	rx := len(rf.logs)

	for lx+1 < rx {
		mid := (lx + rx) / 2
		midLog := rf.logs[mid]
		if midLog.Index <= index && midLog.Term <= term {
			lx = mid
		} else {
			rx = mid
		}
	}
	return rf.logs[lx], lx
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0
	isLeader := rf.checkIsLeaderWithoutLock()

	if isLeader {
		_, lastLogIndex := rf.GetLastTermIndexWithoutLock()

		index = lastLogIndex + 1
		newLog := Log{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		rf.logs = append(rf.logs, newLog)
		rf.matchIndex[rf.me] = index
		rf.persist()
	}

	return index, rf.currentTerm, isLeader
}

func (rf *Raft) ticker() {
	for true {
		if !rf.checkLeaderLivenessWithLock() {
			go rf.startElection()
		} else {
			go rf.sendLeaderLivenessPing()
		}

		ms := TICKER_BASE_SLEEP_CYCLE + (rand.Int63() % TICKER_RANDOM_SLEEP_CYCLE)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkLeaderLivenessWithoutLock() bool {
	if rf.checkIsLeaderWithoutLock() {
		return true
	}
	leaderLastActElapsed := time.Since(rf.lastLeaderActivityTime)
	return leaderLastActElapsed < LEADER_AVAILIBILITY+rf.electionRandomTimeout
}

func (rf *Raft) checkLeaderLivenessWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.checkLeaderLivenessWithoutLock()
}

func (rf *Raft) sendLeaderLivenessPing() {
	rf.mu.Lock()
	if !rf.checkIsLeaderWithoutLock() {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		go rf.syncFollower(currentTerm, peerID)
	}
}

func (rf *Raft) upgradeSnapshotState(peerID int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex, _ := rf.getSnapshotIndexTermWithoutLock()
	if rf.nextIndex[peerID] <= snapshotIndex {
		go rf.sendSnapshot(peerID)
		return true
	}
	return false
}

func (rf *Raft) prepareSyncFollower(currentTerm, peerID int) (args AppendEntriesRequest, roundStamp int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLog, prevPos, err := rf.getLogWithIndex(rf.nextIndex[peerID] - 1)

	if err != nil {
		panic(fmt.Errorf("Unexpected Error in catch next Index peer %v with leader %v, err: %s",
			peerID, rf.me, err))
	}

	entries := make([]Log, 0)
	if prevPos < len(rf.logs)-1 {
		entries = append(entries, rf.logs[prevPos+1:]...)
	}

	args = AppendEntriesRequest{
		Term:         currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	roundStamp = rf.syncFollowerStamp[peerID] + 1

	return args, roundStamp
}

func (rf *Raft) proccessSyncFollowerReply(peerID, index, roundStamp int, sendedEntries []Log, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.syncFollowerStamp[peerID] >= roundStamp {
		return
	}
	rf.syncFollowerStamp[peerID] = roundStamp

	if reply.Success {
		lastMatchedIndex := index
		if len(sendedEntries) > 0 {
			lastMatchedIndex = sendedEntries[len(sendedEntries)-1].Index
		}
		rf.matchIndex[peerID] = lastMatchedIndex
		rf.nextIndex[peerID] = lastMatchedIndex + 1
		rf.calculateCommitIndexWithoutLock()
	} else {
		if reply.Term > rf.currentTerm {
			rf.changeRoleToFollowerWithoutLock(reply.Term, true)
		} else {
			confilictLog := reply.Confilict
			prevConfilictLog, _ := rf.getLowerboundLog(confilictLog.LastIndex, confilictLog.LastTerm)

			rf.nextIndex[peerID] = prevConfilictLog.Index + 1
		}
	}

	sIndex, _ := rf.getSnapshotIndexTermWithoutLock()

	if reply.SnapshotIndex < sIndex {
		go rf.sendSnapshot(peerID)
	}
}

func (rf *Raft) syncFollower(currentTerm, peerID int) {
	upgraded := rf.upgradeSnapshotState(peerID)
	if upgraded {
		return
	}
	args, roundStamp := rf.prepareSyncFollower(currentTerm, peerID)

	reply := &AppendEntriesReply{}

	ok := rf.peers[peerID].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.proccessSyncFollowerReply(peerID, args.PrevLogIndex, roundStamp, args.Entries, reply)
}

func (rf *Raft) calculateCommitIndexWithoutLock() {
	sortedMatches := append([]int(nil), rf.matchIndex...)
	slices.Sort(sortedMatches)

	majority := len(rf.peers)/2 + 1
	candidateCommitIndex := sortedMatches[majority-1]

	highestMajorityLog, _, err := rf.getLogWithIndex(candidateCommitIndex)

	if err != nil {
		DPrintf("Unexpected Error in catch next Index peer %v with leader %v for calculate commit index, err: %s",
			rf.me, rf.me, err)
		return
	}

	if highestMajorityLog.Term == rf.currentTerm {
		rf.commitIndex = candidateCommitIndex
		rf.cond.Broadcast()
	}
}

func (rf *Raft) GetLastTermIndexWithoutLock() (lastLogTerm, lastLogIndex int) {
	if len(rf.logs) == 0 {
		lastLogIndex = -1
		lastLogTerm = 0
		return
	}
	lastLog := rf.logs[len(rf.logs)-1]
	lastLogIndex = lastLog.Index
	lastLogTerm = lastLog.Term
	return
}

func (rf *Raft) GetLastTermIndexWithLock() (lastLogTerm, lastLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogTerm, lastLogIndex = rf.GetLastTermIndexWithoutLock()
	return
}

func (rf *Raft) checkIsLeaderWithoutLock() bool {
	return rf.role == Leader
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		votedFor:    -1,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		logs: []Log{
			{Term: 0, Index: 0},
		},
		applyCh:           applyCh,
		role:              Follower,
		syncFollowerStamp: make([]int, len(peers)),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.cond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	snapshotIndex, _ := rf.getSnapshotIndexTermWithoutLock()
	if snapshotIndex > 0 {
		rf.penddingSnapshotaApply = true
		rf.cond.Broadcast()
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	_, lastIndex := rf.GetLastTermIndexWithLock()
	for peerId := range len(rf.peers) {
		rf.nextIndex[peerId] = lastIndex + 1
		rf.matchIndex[peerId] = 0
	}

	rf.changeRoleToFollowerWithoutLock(0, true)

	go rf.ticker()
	go rf.applyCommitedLogs()

	return rf
}
