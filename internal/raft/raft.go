package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GiorgosMarga/dfs/internal/contracts"
)

// TODO: each peer should have its own outbound chan to not block in case of slow servers

const MinPeers = 3

type Role uint8

const HeartbeatInterval = 100 // ms
const (
	Leader Role = iota
	Candidate
	Follower
	PassiveListener
)

type Raft struct {
	// identity
	Id       uint64
	LeaderId uint64

	// this map keeps all active peers (the peers that are currently connected with this node)
	activePeers map[uint64]struct{}
	// this map keeps all peers (the peers that are currently connected with this node or were connected with this node)
	// this map is used for cluster config changes, to still be able to reach nodes that are leaving
	allPeers map[uint64]struct{}

	// only 1 cluster config change at a time can be commited
	latestConfigIndex uint64 // Index of the latest config entry in the log

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
	ApplyCh    chan []byte      // committed entries to apply

	// candidate state
	grantedVotes   int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCond *sync.Cond
	mu        *sync.Mutex
	f         *os.File
	quitCh    chan struct{}
}

func randomDurationMs() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

func NewRaft(id uint64) *Raft {
	f, err := os.OpenFile(fmt.Sprintf(".raft/%d", id), os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		panic(err)
	}

	r := &Raft{
		Id:             id,
		activePeers:    make(map[uint64]struct{}, 0),
		allPeers:       make(map[uint64]struct{}, 0),
		currentTerm:    0,
		lastApplied:    0,
		votedFor:       0,
		log:            NewLog(),
		heartbeatTimer: time.NewTimer(time.Duration(HeartbeatInterval) * time.Millisecond), // random period from 150-300 ms
		nextIndex:      make(map[uint64]uint64),
		matchIndex:     make(map[uint64]uint64),
		InboundCh:      make(chan []byte),
		OutboundCh:     make(chan RaftMessage, 64),
		ApplyCh:        make(chan []byte),
		electionTimer:  time.NewTimer(randomDurationMs()),
		role:           Follower,
		applyCond:      sync.NewCond(&sync.Mutex{}),
		mu:             &sync.Mutex{},
		f:              f,
		quitCh:         make(chan struct{}),
	}

	if err := r.recover(); err != nil {
		panic(err)
	}

	return r
}

func (r *Raft) IsLeader() bool {
	return r.role == Leader
}

func (r *Raft) heartbeatLoop() {
	for {
		select {
		case <-r.heartbeatTimer.C:
			if r.role != Leader {
				return
			}
			r.mu.Lock()
			for peer := range r.allPeers {
				_, isActive := r.activePeers[peer]

				if !isActive && r.latestConfigIndex >= r.nextIndex[peer] {
					fmt.Println(r.latestConfigIndex, r.nextIndex[peer])
					continue
				}
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
		case <-r.quitCh:
			fmt.Printf("[%d]: Closing raft heartbeat loop\n", r.Id)
			return
		}
	}
}

func (r *Raft) recover() error {
	r.f.Seek(0, io.SeekStart)
	// read metadata
	metadata := make([]byte, 16)
	if _, err := r.f.Read(metadata); err != nil {
		if errors.Is(err, io.EOF) {
			// new server, no log
			return nil
		}
		return err
	}

	r.currentTerm = binary.LittleEndian.Uint64(metadata)
	r.votedFor = binary.LittleEndian.Uint64(metadata[8:])
	r.f.Seek(0, io.SeekCurrent)
	for {
		b := make([]byte, 4)
		if _, err := r.f.Read(b); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		entrySize := binary.LittleEndian.Uint32(b)
		entryBuf := make([]byte, entrySize)
		if _, err := r.f.Read(entryBuf); err != nil {
			return err
		}
		entry, err := DecodeLogEntry(entryBuf)
		if err != nil {
			return err
		}
		r.log.append(entry)
	}

}
func (r *Raft) writeMetadataHeader() error {
	if _, err := r.f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	b := make([]byte, 0, 16)
	b = binary.LittleEndian.AppendUint64(b, r.currentTerm)
	b = binary.LittleEndian.AppendUint64(b, r.votedFor)
	if _, err := r.f.Write(b); err != nil {
		return err
	}
	return nil
}
func (r *Raft) appendEntriesOnDisk(entries []LogEntry) error {
	if err := r.writeMetadataHeader(); err != nil {
		return err
	}
	if _, err := r.f.Seek(16, io.SeekStart); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	buf := make([]byte, 0)
	for _, entry := range entries {
		if len(entry.Data) == 0 {
			continue
		}
		b, _ := entry.Encode()
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
		buf = append(buf, b...)
	}
	if _, err := r.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	_, err := r.f.Write(buf)
	if err != nil {
		return err
	}
	return nil
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
				if err := r.handleAppendEntriesResp(raftMsg.Payload, raftMsg.From); err != nil {
					fmt.Printf("[%d]: ERROR %s\n", r.Id, err)
				}
			}
		case <-r.electionTimer.C:
			r.mu.Lock()
			if r.role == Leader || r.role == PassiveListener {
				r.mu.Unlock()
				continue
			}
			r.mu.Unlock()

			r.handleElectionPeriod()
		case <-r.quitCh:
			fmt.Printf("[%d]: Terminating run loop...\n", r.Id)
			return
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
		// Update tracking for this follower
		r.matchIndex[from] = r.log.lastIndex() // TODO: fix this, should be in response
		r.nextIndex[from] = r.matchIndex[from] + 1
		r.maybeAdvanceCommitIndex()
		return nil
	}

	if r.nextIndex[from] > 1 {
		r.nextIndex[from]--
	} else {
		return nil
	}
	// Prepare the retry message
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

		if count >= (len(r.activePeers)/2 + 1) {
			r.commitIndex = n
			r.applyCond.Signal() // wake up the applier
			break
		}
	}
}

func (r *Raft) AddPeer(id uint64) {
	r.activePeers[id] = struct{}{}
	r.allPeers[id] = struct{}{}
}
func (r *Raft) handleElectionPeriod() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.activePeers) == 0 {
		fmt.Println("skipping election")
		return
	}

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
	for peer := range r.activePeers {
		if peer == r.Id {
			continue
		} // Don't send to self
		if err := r.sendMessage(req, peer); err != nil {
			// fmt.Printf("[%d]: Error sending message to %d (%s)\n", r.Id, peer, err)
		}
	}
}

func (r *Raft) handleRequestVoteResp(b []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp, err := DecodeRequestVoteResp(b)
	if err != nil {
		return err
	}
	// If we find a higher term, we are no longer a candidate
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.stepDown()
		return nil
	}

	// Only process the vote if we are still a candidate for THIS term
	if r.role == Candidate && resp.VoteGranted && resp.Term == r.currentTerm {
		r.grantedVotes++
		// Check for majority (N/2 + 1)
		if r.grantedVotes >= (len(r.activePeers)/2 + 1) {
			r.becomeLeader()
		}
	}
	return nil
}
func (r *Raft) becomeLeader() {
	fmt.Printf("[%d]: New Leader\n", r.Id)
	r.role = Leader
	r.LeaderId = r.Id

	// Initialize leader state for all peers
	lastIdx := r.log.lastIndex()
	for peer := range r.activePeers {
		r.nextIndex[peer] = lastIdx + 1
		r.matchIndex[peer] = 0
	}

	// Immediately send heartbeats to establish authority
	go r.heartbeatLoop()
}

func (r *Raft) stepDown() {
	r.role = Follower
	r.LeaderId = 0
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

	// Create a new log entry
	newEntry := LogEntry{
		Index: r.log.lastIndex() + 1,
		Term:  r.currentTerm,
		Data:  data,
	}

	// Append to local log
	r.log.append(newEntry)
	if err := r.appendEntriesOnDisk([]LogEntry{newEntry}); err != nil {
		return 0
	}
	// Update own matchIndex
	r.matchIndex[r.Id] = newEntry.Index

	// Trigger replication to all peers
	for peer := range r.activePeers {
		if peer == r.Id {
			continue
		}
		prevIdx := r.nextIndex[peer] - 1
		prevTerm := r.log.termAt(prevIdx)
		nextIdx := r.nextIndex[peer]
		entriesToReplicate := r.log.slice(nextIdx, r.log.lastIndex())
		go func(p uint64, pIdx uint64, pTerm uint64, ents []LogEntry) {
			r.sendMessage(AppendEntries{
				Term:         r.currentTerm,
				LeaderId:     r.Id,
				PrevLogIndex: pIdx,
				PrevLogTerm:  pTerm,
				LeaderCommit: r.commitIndex,
				Entries:      ents,
			}, p)
		}(peer, prevIdx, prevTerm, entriesToReplicate)
	}

	return r.log.lastIndex()
}
func (r *Raft) ProposeConfigChange(data []byte) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != Leader {
		// Return error or redirect to actual leader
		return 0
	}

	// Create a new log entry
	newEntry := LogEntry{
		Index:     r.log.lastIndex() + 1,
		Term:      r.currentTerm,
		Data:      data,
		EntryType: ChangeConfig,
	}
	fmt.Printf("New entry: %+v\n", newEntry)

	// Append to local log
	r.log.append(newEntry)
	if err := r.appendEntriesOnDisk([]LogEntry{newEntry}); err != nil {
		return 0
	}
	r.latestConfigIndex = newEntry.Index

	// Update own matchIndex
	r.matchIndex[r.Id] = newEntry.Index

	// Trigger replication to all peers
	for peer := range r.activePeers {
		if peer == r.Id {
			continue
		}
		prevIdx := r.nextIndex[peer] - 1
		prevTerm := r.log.termAt(prevIdx)
		nextIdx := r.nextIndex[peer]
		entriesToReplicate := r.log.slice(nextIdx, r.log.lastIndex())
		go func(p uint64, pIdx uint64, pTerm uint64, ents []LogEntry) {
			r.sendMessage(AppendEntries{
				Term:         r.currentTerm,
				LeaderId:     r.Id,
				PrevLogIndex: pIdx,
				PrevLogTerm:  pTerm,
				LeaderCommit: r.commitIndex,
				Entries:      ents,
			}, p)
		}(peer, prevIdx, prevTerm, entriesToReplicate)
	}
	if newEntry.EntryType == ChangeConfig {
		if err := r.handleConfigChangeEntry(newEntry); err != nil {
			fmt.Println(err)
			return 0
		}
	}
	return r.log.lastIndex()
}
func (r *Raft) Close() error {
	// close(r.OutboundCh)
	close(r.ApplyCh)
	return nil
}

func (r *Raft) handleAppendEntries(msg []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, err := DecodeAppendEntries(msg)
	if err != nil {
		return err
	}
	if req.Term < r.currentTerm {
		return r.sendMessage(AppendEntriesResp{
			Success: false,
			Term:    r.currentTerm,
		}, req.LeaderId)
	}
	r.electionTimer.Reset(randomDurationMs())

	r.LeaderId = req.LeaderId
	// If term is newer, update local state
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.role = Follower
		r.votedFor = 0
		r.LeaderId = req.LeaderId
	}

	// Reply false if log doesn’t contain an entry at PrevLogIndex
	// whose term matches PrevLogTerm
	if !r.log.hasMatchingEntry(req.PrevLogIndex, req.PrevLogTerm) {
		fmt.Printf("[NODE %d ERROR] Match Failed! Leader sent PrevIndex: %d, PrevTerm: %d. "+
			"My Local Log LastIndex: %d, LastTerm: %d, TermAtPrevIndex: %d\n",
			r.Id, req.PrevLogIndex, req.PrevLogTerm,
			r.log.lastIndex(), r.log.lastTerm(), r.log.termAt(req.PrevLogIndex))
		return r.sendMessage(AppendEntriesResp{
			Success: false,
			Term:    r.currentTerm,
		}, req.LeaderId)
	}

	toWrite := make([]LogEntry, 0)
	// Append new entries and resolve conflicts
	for _, entry := range req.Entries {
		// If we don't have this index yet, just append everything from here on
		if entry.Index > r.log.lastIndex() {
			r.log.append(entry)
			toWrite = append(toWrite, entry)
			if entry.EntryType == ChangeConfig {
				if err := r.handleConfigChangeEntry(entry); err != nil {
					fmt.Println(err)
					return err
				}
			}
			continue
		}

		// If we HAVE the index, check if the terms match
		existingTerm := r.log.termAt(entry.Index)
		if existingTerm != entry.Term {
			// CONFLICT: The leader is right, we are wrong.
			r.log.truncateFromIndex(entry.Index) // truncate from file as well
			toWrite = append(toWrite, entry)
			r.log.append(entry)
			if entry.EntryType == ChangeConfig {
				if err := r.handleConfigChangeEntry(entry); err != nil {
					fmt.Println(err)
					return err
				}
			}
		}
	}
	if err := r.appendEntriesOnDisk(toWrite); err != nil {
		return err
	}
	// Update commitIndex
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
	if r.role == PassiveListener {
		return r.sendMessage(RequestVoteResp{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, req.CandidateId)
	}

	if req.Term < r.currentTerm {
		return r.sendMessage(RequestVoteResp{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, req.CandidateId)
	}

	// If candidate's term is newer, update local state and step down
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.role = Follower
		r.votedFor = 0
		// Note: Do NOT reset election timer here yet; only reset if you grant the vote
	}

	// Check if we can grant the vote
	canVote := (r.votedFor == 0 || r.votedFor == req.CandidateId)
	// Check Log Up-to-Date property
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

func (r *Raft) handleConfigChangeEntry(entry LogEntry) error {
	if entry.EntryType != ChangeConfig {
		return fmt.Errorf("invalid entry")
	}
	cmd, err := contracts.DecodeServerCommand(entry.Data)
	if err != nil {
		fmt.Println(err)
		return err
	}
	cc, err := contracts.DecodeClusterConfig(cmd.Payload)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if !slices.Contains(cc.ClusterNodes, r.Id) {
		fmt.Println("passive listener")
		// i am a passive listeners, i dont start elections (propably waiting for my removal)
		r.role = PassiveListener
	}
	r.latestConfigIndex = entry.Index
	r.activePeers = make(map[uint64]struct{})

	for _, id := range cc.ClusterNodes {
		if id == r.Id {
			continue
		}
		r.activePeers[id] = struct{}{}
	}
	return nil
}

func (r *Raft) applyLoop() {
	for {

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

			r.ApplyCh <- entry.Data
			r.mu.Lock()
			r.lastApplied++
			r.mu.Unlock()
		}
	}
}
func (r *Raft) Consume() <-chan []byte {
	return r.ApplyCh
}

func (r *Raft) RemovePeer(peerId uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.allPeers, peerId)
}
