package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicate   = "ErrDuplicate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	RequestId uint32
	ClerkId   uint32
}

type PutAppendReply struct {
	ServerID int
	Err      Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
