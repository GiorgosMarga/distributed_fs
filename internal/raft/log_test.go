package raft

import (
	"fmt"
	"testing"
)

func TestLog(t *testing.T) {
	log := NewLog()
	log.append(LogEntry{Index: 1})
	log.append(LogEntry{Index: 2})
	log.append(LogEntry{Index: 3})

	fmt.Println(log.slice(1, log.lastIndex()))
}

func TestEntryEncodeDecode(t *testing.T) {
	entry := LogEntry{
		Index: 10,
		Term:  1,
		Data:  []byte("hello"),
	}

	b, _ := entry.Encode()

	newEntry, _ := DecodeLogEntry(b)

	fmt.Printf("%+v\n", newEntry)
}
