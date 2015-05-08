package pbservice

import (
"fmt"
"encoding/hex"
"crypto/rand"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	PUT = "Put"
)

const Debug = 0

func DPrintln(format string) {
        if Debug > 0 {
		fmt.Println(format)
        }
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                n, err = fmt.Printf(format, a...)
        }
        return
}


type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	Op string
	Guid string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ReplicateArgs struct {
	Key string
}

type ReplicateReply struct {
	Store map[string]string
	GuidMap map[string]string
}


func GenUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := rand.Read(uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// TODO: verify the two lines implement RFC 4122 correctly
	uuid[8] = 0x80 // variant bits see page 5
	uuid[4] = 0x40 // version 4 Pseudo Random, see page 7

	return hex.EncodeToString(uuid), nil
}


// Your RPC definitions here.
