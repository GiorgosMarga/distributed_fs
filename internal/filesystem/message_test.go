package filesystem

import (
	"bytes"
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
