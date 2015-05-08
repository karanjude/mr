package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"


type Clerk struct {
	vs *viewservice.Clerk
	Primary string
	// Your declarations here

	Retry int
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck* Clerk) GetView() {
	if ck.Primary != "" {
		return
	}

	view, ok := ck.vs.Get()

	if ok == true {
		ck.Primary = view.Primary
	}

	//fmt.Println("Got view :%v", view)	
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	ck.Retry = 0
	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	ck.GetView()

	args := &GetArgs{}
	args.Key = key
	
	var reply GetReply
	call(ck.Primary, "PBServer.Get", args, &reply)

//	fmt.Printf("\n GET : %v ", ck)

	if reply.Err == OK {
		ck.Retry = 0
		return reply.Value
	}else{

		for {
			view, ok := ck.vs.Get()
			if ok == false {
				continue
			}

			ck.Primary = view.Primary
			call(ck.Primary, "PBServer.Get", args, &reply)

			if(reply.Err == OK){
				break
			}

		}

		return reply.Value			
	}

	return ErrNoKey
}

func (ck *Clerk) PutAppendRPC(args *PutAppendArgs, reply *PutAppendReply) bool{
	ok := call(ck.Primary, "PBServer.PutAppend", args, reply)

	for {
		if ok == true {
			//fmt.Println("Append successful")
			break
		}

		//fmt.Println("Append unsuccessful")
		ok = ck.PutAppendRPC(args, reply)
	}
	
	return ok
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.GetView()

	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Guid, _ = GenUUID()
	
	var reply PutAppendReply
	
	ck.PutAppendRPC(args, &reply)
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
