package session

import "testing"

func TestTransactionPinUsesBackendReply(t *testing.T) {
	s := &Session{}
	s.observeTransactionReply("MULTI", []byte("-ERR bad args\r\n"))
	if s.inMulti {
		t.Fatal("failed MULTI pinned transaction")
	}
	s.observeTransactionReply("WATCH", []byte("+OK\r\n"))
	s.observeTransactionReply("MULTI", []byte("+OK\r\n"))
	s.observeTransactionReply("UNWATCH", []byte("+QUEUED\r\n"))
	if !s.inMulti || !s.watching {
		t.Fatal("queued UNWATCH unpinned transaction")
	}
	s.observeTransactionReply("EXEC", []byte("-ERR wrong arguments\r\n"))
	if !s.inMulti {
		t.Fatal("malformed EXEC unpinned transaction")
	}
	s.observeTransactionReply("EXEC", []byte("*-1\r\n"))
	if s.inMulti || s.watching {
		t.Fatal("aborted WATCH not released")
	}
	s.observeTransactionReply("MULTI", []byte("+OK\r\n"))
	s.observeTransactionReply("EXEC", []byte("-EXECABORT previous errors\r\n"))
	if s.inMulti {
		t.Fatal("EXECABORT not released")
	}
}
