package raft

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

type Role uint8

const (
	Leader Role = iota
	Candidate
	Follower
)

type Raft struct {
	// identity
	id    uint64
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
	inboundCh  chan []byte   // messages from network
	outboundCh chan []byte   // messages to send
	applyCh    chan LogEntry // committed entries to apply

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func NewRaft(id uint64) *Raft {
	return &Raft{
		id:             id,
		peers:          make([]uint64, 0),
		currentTerm:    0,
		lastApplied:    0,
		votedFor:       0,
		log:            NewLog(),
		heartbeatTimer: time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond), // random period from 150-300 ms
		nextIndex:      make(map[uint64]uint64),
		matchIndex:     make(map[uint64]uint64),
		inboundCh:      make(chan []byte),
		outboundCh:     make(chan []byte),
		applyCh:        make(chan LogEntry),
		electionTimer:  time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond),
		role:           Follower,
	}
}

func (r *Raft) readLoop() {
	for msg := range r.inboundCh {
		msgType := binary.LittleEndian.Uint32(msg)
		switch MsgType(msgType) {
		case MsgAppendEntries:
			if err := r.handleAppendEntries(msg[4:]); err != nil {
				fmt.Printf("[%d]: ERROR %s\n", r.id, err)
			}
		case MsgRequstVote:
			if err := r.handleRequestVote(msg[4:]); err != nil {
				fmt.Printf("[%d]: ERROR %s\n", r.id, err)
			}
		}
	}
}

func (r *Raft) handleAppendEntries(msg []byte) error {
	appendEntriesMsg, err := DecodeAppendEntries(msg)
	if err != nil {
		return err
	}
	var resp AppendEntriesResp
	term := r.log.termAt(appendEntriesMsg.PrevLogIndex)
	if appendEntriesMsg.Term < r.currentTerm || term != appendEntriesMsg.PrevLogTerm {
		resp = AppendEntriesResp{
			Success: false,
			Term:    r.currentTerm,
		}
	} else {
		var idxOfLastNewEntry uint64 = 0
		for _, entry := range appendEntriesMsg.Entries {
			term := r.log.termAt(entry.Index)
			if term != entry.Term {
				r.log.truncateFromIndex(entry.Index)
			}
			if r.log.lastIndex()+1 == entry.Index {
				r.log.append(entry)
				idxOfLastNewEntry = entry.Index
			}
		}

		if appendEntriesMsg.LeaderCommit > r.commitIndex {
			r.commitIndex = min(appendEntriesMsg.LeaderCommit, idxOfLastNewEntry)
		}
		resp = AppendEntriesResp{
			Success: true,
			Term:    r.currentTerm,
		}

	}
	respBuf, err := createResponse(resp, appendEntriesMsg.LeaderId)
	if err != nil {
		return err
	}
	r.outboundCh <- respBuf
	return nil
}

func (r *Raft) handleRequestVote(msg []byte) error {
	requestVoteMsg, err := DecodeRequestVote(msg)
	if err != nil {
		return err
	}

	var resp RequestVoteResp
	if (r.votedFor == 0 || r.votedFor == requestVoteMsg.CandidateId) && requestVoteMsg.LastLogIndex >= r.commitIndex {
		resp = RequestVoteResp{
			Term:        r.currentTerm,
			VoteGranted: true,
		}
	} else {
		resp = RequestVoteResp{
			VoteGranted: false,
			Term:        r.currentTerm,
		}
	}
	respBuf, err := createResponse(resp, requestVoteMsg.CandidateId)
	if err != nil {
		return err
	}
	r.outboundCh <- respBuf
	return nil
}

func (r *Raft) Consume() []byte {
	data := <-r.applyCh
	b, _ := data.Encode()
	return b
}
