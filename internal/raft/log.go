package raft

import (
	"encoding/binary"
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

func (le LogEntry) Encode() ([]byte, error) {
	b := make([]byte, 0, 24+len(le.Data))
	b = binary.LittleEndian.AppendUint64(b, le.Index)
	b = binary.LittleEndian.AppendUint64(b, le.Term)
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
	dataLen := binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	data := make([]byte, dataLen)
	copy(data, b[offset:])
	logEntry.Data = data
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
	*l = (*l)[:index]
}

func (l Log) lastIndex() uint64 {
	return uint64(len(l))
}
func (l Log) termAt(idx uint64) uint64 {
	if idx >= uint64(len(l)) {
		return 0
	}
	return l[idx].Term
}

func (l Log) slice(from, to int) []LogEntry {
	if from >= to || from <= 0 || to >= len(l) {
		return nil
	}
	return l[from:to]
}
