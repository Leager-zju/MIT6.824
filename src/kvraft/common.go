package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicate   = "ErrDuplicate"
)

type Err string

type Args struct {
	Key       string
	Value     string
	Op        string // "Get" "Put" or "Append"
	RequestId uint32
	ClerkId   uint32

	Ch chan *Reply
}

type Reply struct {
	Value string
	Err   Err
}
