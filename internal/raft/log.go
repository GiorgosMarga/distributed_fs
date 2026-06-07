package raft

import (
	"encoding/binary"
)

type LogEntryType byte

const (
	Normal LogEntryType = iota
	ChangeConfig
)

type LogEntry struct {
	Index     uint64
	Term      uint64
	Data      []byte
	EntryType LogEntryType
}

func (le LogEntry) Encode() ([]byte, error) {
	b := make([]byte, 0, 25+len(le.Data))
	b = binary.LittleEndian.AppendUint64(b, le.Index)
	b = binary.LittleEndian.AppendUint64(b, le.Term)
	b = append(b, byte(le.EntryType))
	b = binary.LittleEndian.AppendUint64(b, uint64(len(le.Data)))
	b = append(b, le.Data...)
	return b, nil
}
func DecodeLogEntry(b []byte) (LogEntry, error) {
	logEntry := LogEntry{}
	offset := 0
	logEntry.Index = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	logEntry.Term = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	logEntry.EntryType = LogEntryType(b[offset])
	offset++
	dataLen := binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	logEntry.Data = make([]byte, dataLen)
	copy(logEntry.Data, b[offset:])
	return logEntry, nil
}

type Log []LogEntry

func NewLog() Log {
	return make(Log, 0)
}

func (l *Log) append(entry LogEntry) {
	*l = append(*l, entry)
}
func (l *Log) truncateFromIndex(index uint64) {
	if index == 0 || len(*l) == 0 {
		return
	}
	index -= 1 // make it zero based
	*l = (*l)[:index]
}

func (l Log) lastIndex() uint64 {
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1].Index
}

func (l Log) lastTerm() uint64 {
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1].Term
}
func (l Log) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	index -= 1 // make it zero based
	if index >= uint64(len(l)) {
		return 0
	}
	return l[index].Term
}

func (l Log) slice(from, to uint64) []LogEntry {
	if from > to || from == 0 || to > uint64(len(l)) {
		return nil
	}
	return l[from-1:to]
}
func (l Log) hasMatchingEntry(prevIndex uint64, prevTerm uint64) bool {
	if prevIndex == 0 {
		return true
	}

	if prevIndex > l.lastIndex() {
		return false
	}

	return l.termAt(prevIndex) == prevTerm
}
