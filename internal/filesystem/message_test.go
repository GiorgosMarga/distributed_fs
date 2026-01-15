package filesystem

import (
	"bytes"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestCopy(t *testing.T) {
	ts := time.Now().Unix()
	m := Message{
		ID:        10,
		From:      []byte("FromTest"),
		To:        []byte("ToTest"),
		Timestamp: ts,
	}

	buf, err := m.Encode()
	if err != nil {
		t.Error(err)
	}

	m2, err := DecodeMessage(buf)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(m.From, m2.From) {
		t.Error("Wrong from")
	}

	if !bytes.Equal(m.To, m2.To) {
		t.Error("Wrong To")
	}

	if m.ID != m2.ID {
		t.Error("Wrong id")
	}

	if m.Timestamp != m2.Timestamp {
		t.Error("Wrong ts")
	}

}

func TestMetaDataMsg(t *testing.T) {
	msg := MetadataMessage{
		MetadataEntry: MetadataEntry{
			Name:     "test name",
			Size:     1024,
			ChunkIDs: []string{"1", "20", "300"},
			Replicas: map[string][]string{
				"1":   {"server1", "server2"},
				"20":  {"server2", "server3"},
				"300": {"server3", "server4"},
			},
		},
	}

	b := msg.Encode()
	fmt.Println(b)
	msg2, err := DecodeMetadataMsg(b)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", msg2)

}
