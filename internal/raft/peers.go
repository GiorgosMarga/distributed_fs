package raft

type Peer struct {
	addr     []byte
	isLeader bool
}
