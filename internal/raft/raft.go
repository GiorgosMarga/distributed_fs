package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const MinPeers = 3

type Role uint8

const HeartbeatInterval = 100 // ms
const (
	Leader Role = iota
	Candidate
	Follower
)

type Raft struct {
	// identity
	Id    uint64
	peers []uint64

	// should be persistent
	currentTerm uint64
	votedFor    uint64
	log         Log

	// volatile
	commitIndex uint64 // up to which log index is commited
	lastApplied uint64 // up to which log index its applied (sent to upper layer)

	// leader only
	nextIndex  map[uint64]uint64 // next log index to send to peer p
	matchIndex map[uint64]uint64 // highest log index known to be replicated on peer p

	role       Role
	InboundCh  chan []byte      // messages from network
	OutboundCh chan RaftMessage // messages to send
	ApplyCh    chan LogEntry    // committed entries to apply

	// candidate state
	grantedVotes   int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCond *sync.Cond
	mu        *sync.Mutex
}

func randomDurationMs() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

func NewRaft(Id uint64) *Raft {
	r := &Raft{
		Id:             Id,
		peers:          make([]uint64, 0),
		currentTerm:    0,
		lastApplied:    0,
		votedFor:       0,
		log:            NewLog(),
		heartbeatTimer: time.NewTimer(time.Duration(HeartbeatInterval) * time.Millisecond), // random period from 150-300 ms
		nextIndex:      make(map[uint64]uint64),
		matchIndex:     make(map[uint64]uint64),
		InboundCh:      make(chan []byte),
		OutboundCh:     make(chan RaftMessage),
		ApplyCh:        make(chan LogEntry),
		electionTimer:  time.NewTimer(randomDurationMs()),
		role:           Follower,
		applyCond:      sync.NewCond(&sync.Mutex{}),
		mu:             &sync.Mutex{},
	}
	return r
}

func (r *Raft) IsLeader() bool {
	return r.role == Leader
}

func (r *Raft) heartbeatLoop() {
	for range r.heartbeatTimer.C {
		if r.role != Leader {
			return
		}
		r.mu.Lock()
		for _, peer := range r.peers {
			// fmt.Printf("[%d]: Sending heartbeat to %d\n", r.Id, peer)
			if err := r.sendMessage(AppendEntries{
				Term:         r.currentTerm,
				LeaderId:     r.Id,
				PrevLogIndex: r.log.lastIndex(),
				PrevLogTerm:  r.log.termAt(r.log.lastIndex()),
				LeaderCommit: r.commitIndex,
				Entries:      []LogEntry{},
			}, peer); err != nil {
				fmt.Println(err)
				continue
			}
		}
		r.heartbeatTimer.Reset(time.Duration(HeartbeatInterval) * time.Millisecond)
		r.mu.Unlock()
	}
}

func (r *Raft) Run() {
	fmt.Printf("[%d]: Raft started\n", r.Id)
	go r.applyLoop()
	for {
		select {
		case msg := <-r.InboundCh:
			raftMsg, err := DecodeRaftMessage(msg)
			if err != nil {
				fmt.Println(err)
				continue
			}
			switch raftMsg.Type {
			case MsgAppendEntries:
				if err := r.handleAppendEntries(raftMsg.Payload); err != nil {
					fmt.Printf("[%d]: ERROR %s\n", r.Id, err)
				}
			case MsgRequestVote:
				if err := r.handleRequestVote(raftMsg.Payload); err != nil {
					fmt.Printf("[%d]: ERROR %s\n", r.Id, err)
				}
			case MsgRequestVoteResp:
				if err := r.handleRequestVoteResp(raftMsg.Payload); err != nil {
					fmt.Printf("[%d]: ERROR %s\n", r.Id, err)
				}
			case MsgAppendEntriesResp:
				// fmt.Printf("[%d]: received append entries response\n", r.Id)
				if err := r.handleAppendEntriesResp(raftMsg.Payload, raftMsg.From); err != nil {
					fmt.Printf("[%d]: ERROR %s\n", r.Id, err)
				}
			}
		case <-r.electionTimer.C:
			r.mu.Lock()
			if r.role == Leader {
				r.mu.Unlock()
				continue
			}
			r.mu.Unlock()

			r.handleElectionPeriod()
		}
	}

}

func (r *Raft) handleAppendEntriesResp(msg []byte, from uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp, err := DecodeAppendEntriesResp(msg)
	if err != nil {
		return nil
	}
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.stepDown()
		return nil
	}

	if r.role != Leader {
		return nil
	}

	if resp.Success {
		// 2. Update tracking for this follower
		// We assume the leader sent entries up to r.log.lastIndex()
		r.matchIndex[from] = r.log.lastIndex()
		r.nextIndex[from] = r.matchIndex[from] + 1
		r.maybeAdvanceCommitIndex()
		return nil
	}
	if r.nextIndex[from] > 1 {
		r.nextIndex[from]--
	} else {
		fmt.Println("here")
		return nil
	}
	// 2. Prepare the retry message
	prevIndex := r.nextIndex[from] - 1
	prevTerm := r.log.termAt(prevIndex) // Get term for that specific index
	// Resend AppendEntries with the decremented nextIndex
	return r.sendMessage(AppendEntries{
		Term:         r.currentTerm,
		LeaderId:     r.Id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: r.commitIndex,
		Entries:      r.log.slice(r.nextIndex[from], r.log.lastIndex()),
	}, from)
}
func (r *Raft) maybeAdvanceCommitIndex() {
	// Look for an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm.
	for n := r.log.lastIndex(); n > r.commitIndex; n-- {
		if r.log.termAt(n) != r.currentTerm {
			continue
		}

		count := 1 // Count ourselves
		for _, mIdx := range r.matchIndex {
			if mIdx >= n {
				count++
			}
		}

		if count >= (len(r.peers)/2 + 1) {
			r.commitIndex = n
			r.applyCond.Signal() // Wake up the applier!
			break
		}
	}
}

func (r *Raft) AddPeer(id uint64) {
	r.peers = append(r.peers, id)
}
func (r *Raft) handleElectionPeriod() {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("[%d]: Starting an election\n", r.Id)

	r.role = Candidate
	r.currentTerm++
	r.votedFor = r.Id // Vote for self

	r.grantedVotes = 1
	// Reset timer with a random duration to prevent split votes
	r.electionTimer.Reset(randomDurationMs())
	lastIdx := r.log.lastIndex()
	req := RequestVote{
		Term:         r.currentTerm,
		CandidateId:  r.Id,
		LastLogIndex: lastIdx,
		LastLogTerm:  r.log.termAt(lastIdx),
	}
	for _, peer := range r.peers {
		if peer == r.Id {
			continue
		} // Don't send to self
		go func() {
			if err := r.sendMessage(req, peer); err != nil {
				fmt.Printf("[%d]: Error sending message to %d (%s)\n", r.Id, peer, err)
			}
		}()
	}
}

func (r *Raft) handleRequestVoteResp(b []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp, err := DecodeRequestVoteResp(b)
	if err != nil {
		return err
	}
	// 1. If we find a higher term, we are no longer a candidate
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.stepDown()
		return nil
	}

	// 2. Only process the vote if we are still a candidate for THIS term
	if r.role == Candidate && resp.VoteGranted && resp.Term == r.currentTerm {
		r.grantedVotes++
		// 3. Check for majority (N/2 + 1)
		if r.grantedVotes >= (len(r.peers)/2 + 1) {
			r.becomeLeader()
		}
	}
	return nil
}
func (r *Raft) becomeLeader() {
	fmt.Printf("[%d]: New Leader\n", r.Id)
	r.role = Leader

	// Initialize leader state for all peers
	lastIdx := r.log.lastIndex()
	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIdx + 1
		r.matchIndex[peer] = 0
	}

	// Immediately send heartbeats to establish authority
	go r.heartbeatLoop()
}

func (r *Raft) stepDown() {
	r.role = Follower
	r.votedFor = 0
	r.electionTimer.Reset(randomDurationMs())
}
func (r *Raft) Propose(data []byte) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != Leader {
		// Return error or redirect to actual leader
		return 0
	}

	// 1. Create a new log entry
	newEntry := LogEntry{
		Index: r.log.lastIndex() + 1,
		Term:  r.currentTerm,
		Data:  data,
	}

	// 2. Append to local log
	r.log.append(newEntry)

	// 3. Update own matchIndex
	r.matchIndex[r.Id] = newEntry.Index

	// 4. Trigger replication to all peers
	for _, peer := range r.peers {
		if peer == r.Id {
			continue
		}
		prevIdx := r.nextIndex[peer] - 1
		prevTerm := r.log.termAt(prevIdx)
		go r.sendMessage(AppendEntries{
			Term:         r.currentTerm,
			LeaderId:     r.Id,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			LeaderCommit: r.commitIndex,
			Entries:      r.log.slice(r.nextIndex[peer], r.log.lastIndex()),
		}, peer)
	}
	return r.log.lastIndex()
}

func (r *Raft) handleAppendEntries(msg []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, err := DecodeAppendEntries(msg)
	if err != nil {
		return err
	}
	if len(req.Entries) > 0 {
		fmt.Printf("[%d]: Append entries msg%+v\n", r.Id, req)
	}
	if req.Term < r.currentTerm {
		return r.sendMessage(AppendEntriesResp{
			Success: false,
			Term:    r.currentTerm,
		}, req.LeaderId)
	}
	r.electionTimer.Reset(randomDurationMs())

	// 2. If term is newer, update local state
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.role = Follower
		r.votedFor = 0
	}

	// 3. Reply false if log doesn’t contain an entry at PrevLogIndex
	// whose term matches PrevLogTerm
	if !r.log.hasMatchingEntry(req.PrevLogIndex, req.PrevLogTerm) {
		return r.sendMessage(AppendEntriesResp{
			Success: false,
			Term:    r.currentTerm,
		}, req.LeaderId)
	}

	// 4. Append new entries and resolve conflicts
	for _, entry := range req.Entries {
		// 1. If we don't have this index yet, just append everything from here on
		if entry.Index > r.log.lastIndex() {
			r.log.append(entry)
			continue
		}

		// 2. If we HAVE the index, check if the terms match
		existingTerm := r.log.termAt(entry.Index)
		if existingTerm != entry.Term {
			// CONFLICT: The leader is right, we are wrong.
			r.log.truncateFromIndex(entry.Index)
			r.log.append(entry)
		}
	}

	// 5. Update commitIndex
	if req.LeaderCommit > r.commitIndex {
		// commitIndex = min(leaderCommit, index of last NEW entry)
		lastIdx := r.log.lastIndex()
		r.commitIndex = min(req.LeaderCommit, lastIdx)
		r.applyCond.Signal()
	}

	return r.sendMessage(AppendEntriesResp{
		Success: true,
		Term:    r.currentTerm,
	}, req.LeaderId)
}

func (r *Raft) handleRequestVote(msg []byte) error {
	req, err := DecodeRequestVote(msg)
	if err != nil {
		return err
	}

	if req.Term < r.currentTerm {
		return r.sendMessage(RequestVoteResp{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, req.CandidateId)
	}

	// 2. If candidate's term is newer, update local state and step down
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.role = Follower
		r.votedFor = 0
		// Note: Do NOT reset election timer here yet; only reset if you grant the vote
	}

	// 3. Check if we can grant the vote
	canVote := (r.votedFor == 0 || r.votedFor == req.CandidateId)
	// 4. Check Log Up-to-Date property
	myLastIdx := r.log.lastIndex()
	myLastTerm := r.log.termAt(myLastIdx)
	logIsUpToDate := false
	if req.LastLogTerm > myLastTerm {
		logIsUpToDate = true
	} else if req.LastLogTerm == myLastTerm && req.LastLogIndex >= myLastIdx {
		logIsUpToDate = true
	}

	if canVote && logIsUpToDate {
		r.votedFor = req.CandidateId
		r.electionTimer.Reset(randomDurationMs()) // Reset timer ONLY on granting vote
		return r.sendMessage(RequestVoteResp{
			Term:        r.currentTerm,
			VoteGranted: true,
		}, req.CandidateId)
	}

	return r.sendMessage(RequestVoteResp{
		Term:        r.currentTerm,
		VoteGranted: false,
	}, req.CandidateId)
}

func (r *Raft) applyLoop() {
	for {
		// lastApplied := r.lastApplied
		// commitIndex := r.commitIndex
		r.applyCond.L.Lock()
		for r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		r.applyCond.L.Unlock()

		firstIndex := r.lastApplied + 1
		lastIdx := r.commitIndex
		entriesToApply := r.log.slice(firstIndex, lastIdx)
		// r.mu.Unlock()
		for _, entry := range entriesToApply {
			r.ApplyCh <- entry
			r.mu.Lock()
			r.lastApplied++
			r.mu.Unlock()
		}
	}
}
func (r *Raft) Consume() []byte {
	entry := <-r.ApplyCh
	return entry.Data
}
