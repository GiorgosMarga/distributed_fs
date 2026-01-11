package raft

import "encoding/binary"

type MsgType uint32

const (
	MsgRequstVote MsgType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
)

type RaftMessage struct {
	To      uint64
	Type    MsgType
	Payload []byte
}

func (rm RaftMessage) Encode() ([]byte, error) {
	b := make([]byte, 0, 8+4+8+len(rm.Payload))
	b = binary.LittleEndian.AppendUint64(b, rm.To)
	b = binary.LittleEndian.AppendUint32(b, uint32(rm.Type))
	b = binary.LittleEndian.AppendUint64(b, uint64(len(rm.Payload)))
	b = append(b, rm.Payload...)
	return b, nil
}

func DecodeRaftMessage(b []byte) (RaftMessage, error) {
	rm := RaftMessage{}
	offset := 0
	rm.To = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	rm.Type = MsgType(binary.LittleEndian.Uint32(b[offset:]))
	offset += 4
	payloadLen := binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	payload := make([]byte, payloadLen)
	copy(payload, b[offset:])
	rm.Payload = payload
	return rm, nil
}

type RequestVote struct {
	Term         uint64 // candidate's term
	CandidateId  uint64 // candidate requesting vote
	LastLogIndex uint64 // index of candidate's last log entry
	LastLogTerm  uint64 // term of candidate's last log entry
}

func DecodeRequestVote(b []byte) (RequestVote, error) {
	rv := RequestVote{}
	offset := 0
	rv.Term = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	rv.CandidateId = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	rv.LastLogIndex = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	rv.LastLogTerm = binary.LittleEndian.Uint64(b[offset:])
	return rv, nil
}
func (rv RequestVote) Encode() ([]byte, error) {
	b := make([]byte, 0, 8*4)
	b = binary.LittleEndian.AppendUint64(b, rv.Term)
	b = binary.LittleEndian.AppendUint64(b, rv.CandidateId)
	b = binary.LittleEndian.AppendUint64(b, rv.LastLogIndex)
	b = binary.LittleEndian.AppendUint64(b, rv.LastLogTerm)
	return b, nil
}

type RequestVoteResp struct {
	Term        uint64 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
}

func (resp RequestVoteResp) Encode() ([]byte, error) {
	return nil, nil
}

type AppendEntries struct {
	Term         uint64     // leaders term
	LeaderId     uint64     // so follower can redirect clients
	PrevLogIndex uint64     //index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of prevLogIndex entry
	LeaderCommit uint64     // leaders commit index
	Entries      []LogEntry // empty for heartbeat
}

func (ae *AppendEntries) Encode() ([]byte, error) {
	b := make([]byte, 0, 48)
	b = binary.LittleEndian.AppendUint64(b, ae.Term)
	b = binary.LittleEndian.AppendUint64(b, ae.LeaderId)
	b = binary.LittleEndian.AppendUint64(b, ae.PrevLogIndex)
	b = binary.LittleEndian.AppendUint64(b, ae.PrevLogTerm)
	b = binary.LittleEndian.AppendUint64(b, ae.LeaderCommit)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(ae.Entries)))
	for _, entry := range ae.Entries {
		entryBuf, err := entry.Encode()
		if err != nil {
			return nil, err
		}
		b = binary.LittleEndian.AppendUint64(b, uint64(len(entryBuf)))
		b = append(b, entryBuf...)
	}
	return b, nil
}
func DecodeAppendEntries(b []byte) (AppendEntries, error) {
	ae := AppendEntries{}
	offset := 0
	ae.Term = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	ae.LeaderId = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	ae.PrevLogIndex = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	ae.PrevLogIndex = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	ae.LeaderCommit = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	numOfEntries := binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	ae.Entries = make([]LogEntry, numOfEntries)
	for i := range numOfEntries {
		entry, err := DecodeLogEntry(b[offset:])
		if err != nil {
			return AppendEntries{}, err
		}
		ae.Entries[i] = entry
		offset += 24 + len(entry.Data)
	}
	return ae, nil
}

type AppendEntriesResp struct {
	Success bool   //true if follower contained entry matching prevLogIndex and prevLogTerm
	Term    uint64 // currentTerm, for leader to update itself
}

func (r *AppendEntriesResp) Encode() ([]byte, error) {
	b := make([]byte, 9)
	if r.Success == false {
		b[0] = 0
	} else {
		b[0] = 1
	}
	binary.LittleEndian.PutUint64(b[1:], r.Term)
	return b, nil
}

func createResponse(msg any, to uint64) ([]byte, error) {
	resp := RaftMessage{
		To: to,
	}
	var (
		b   []byte
		err error
	)
	switch t := msg.(type) {
	case RequestVoteResp:
		resp.Type = MsgRequestVoteResp
		b, err = t.Encode()
	case AppendEntriesResp:
		resp.Type = MsgAppendEntriesResp
		b, err = t.Encode()
	}
	if err != nil {
		return nil, err
	}
	resp.Payload = b
	return resp.Encode()
}
