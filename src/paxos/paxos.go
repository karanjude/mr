package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

//import "strconv"
import "container/heap"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type ValueState struct {
	status Fate
	value interface{}
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                n, err = fmt.Printf(format, a...)
        }
        return
}

// An Item is something we manage in a priority queue.
type Item struct {
	value    ValueState // The value of the item; arbitrary.
	N int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

func (item Item) String() string {
	return fmt.Sprintf("{%v : %v}", item.N, item.value)
}


type PriorityQueue []* Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].N > pq[j].N
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value ValueState, priority int) {
	item.value = value
	item.N = priority
	heap.Fix(pq, item.index)
}


type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	n_p int
	n_a int
	v_a interface{}

	prepareState map[PrepareKey]int
	acceptState map[AcceptKey]int
	decidedState map[int]Item
	doneMap map[int]int


	max_seq int
	DoneSeen bool

	state *PriorityQueue
}

type PrepareArgs struct{
	N int
	DoneNumber int
	Id int
}

type AcceptArgs struct{
	N int
	VV interface{}
}

type DecidedArgs struct{
	N int
	VV interface{}
}

const (
	OK = "OK"
	REJECT = "REJECT"
)

type PrepareReply struct {
	N int
	Na int
	Va interface{}

	Err string
	DoneNumber int
}

type PrepareKey struct {
	N int
	Na int
	Va interface{}
}

type AcceptReply struct {
	N int


	Err string
}

type DecidedReply struct {

	Err string
}

type AcceptKey struct {
	N int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) PrepareSeq(args *PrepareArgs, reply *PrepareReply) error {
	DPrintf("\n === (%v) Received prepare(%v)", px.me, args)

	px.DoneFromPeer(args.Id, args.DoneNumber)

	if args.N >= px.n_p {
		px.n_p = args.N
		
		reply.N = args.N
		reply.Na = px.n_a
		reply.Va = px.v_a

		reply.Err = OK
		reply.DoneNumber = px.doneMap[px.me]
	}else{
		reply.Err = REJECT
		reply.DoneNumber = px.doneMap[px.me]
	}

	return nil
}

func (px *Paxos) AcceptSeq(args *AcceptArgs, reply *AcceptReply) error {
	DPrintf("\n --> (%v) Received accept(%v)", px.me, args)

	if args.N >= px.n_p {
		px.n_p = args.N
		px.n_a = args.N
		px.v_a = args.VV
		
		reply.N = px.n_a
		reply.Err = OK

	}else {
		reply.Err = REJECT
	}

	return nil 
}

func (px *Paxos) DecidedSeq(args *DecidedArgs, reply *DecidedReply) error {
	DPrintf("\n --> (%v) Received decided(%v)", px.me, args)

	decidedValueState := Item{}
	decidedValueState.value = ValueState{Decided, args.VV}
	decidedValueState.N = args.N

	px.decidedState[args.N] = decidedValueState

	reply.Err = OK

	DPrintf("\n ==: (%v) state: (%v)", px.me, px.decidedState)

	if args.N > px.max_seq {
		px.max_seq = args.N
	}

	return nil
}

func (px *Paxos) Accept(n int, vv interface{}, seq int) {
	args := &AcceptArgs{}
	args.N = n
	args.VV = vv

	for id, peer := range px.peers {
		var reply AcceptReply

		DPrintf("\n (%v) --> (%v) accept(%v, %v)", px.me, id, args.N, vv)		
		call(peer, "Paxos.AcceptSeq", args, &reply)

		if reply.Err == OK {
			DPrintf("\n (%v) <-- (%v) accept_ok(%v) reply: (%v)", px.me, id, args.N, reply)

			key := AcceptKey{reply.N}
			px.acceptState[key]++

//			DPrintf("\n ==: (%v) acceptState: (%v)", px.me, px.acceptState)

			if px.acceptState[key] >= ((len(px.peers)/2)+1) {
				DPrintf("\n\n ==: (%v) GotMajorty for %v ", px.me, args.N)
				go px.Decided(args.N, vv, seq)

				break
			}
		}
	}
}


func (px *Paxos) Decided(n int, vv interface{}, seq int) {
	args := &DecidedArgs{}
	args.N = seq
	args.VV = vv


	for id, peer := range px.peers {

		var reply DecidedReply
		
		DPrintf("\n (%v) --> (%v) decided(%v, %v)", px.me, id, n, vv)
		call(peer, "Paxos.DecidedSeq", args, &reply)

		if reply.Err == OK {
		}else{
			DPrintf("\n call to Decided %v failed ", id)
		}
	}
}

func (px *Paxos) Prepare(seq int, v interface{}) {
	args := &PrepareArgs{}
	args.N = (seq << 3) + int(time.Now().Unix()) + px.me
	args.DoneNumber = px.doneMap[px.me]
	args.Id = px.me

	NaMax := args.N
	VaMax := v

	for id, peer := range px.peers {
		var reply PrepareReply
		
		DPrintf("\n (%v) --> (%v) prepare(%v) for %v", px.me, id, args.N, v)
		call(peer, "Paxos.PrepareSeq", args, &reply)

		if reply.Err == OK {
			DPrintf("\n(%v) <-- (%v) prepare_ok(%v) reply: (%v)", px.me, id, args.N, reply)

			if reply.Na > NaMax {
				VaMax = reply.Va
			}else{
				VaMax = v
			}

			key := PrepareKey{reply.N, NaMax, VaMax}
			px.prepareState[key]++
			DPrintf("\n ==: (%v) PrepareMajorityState: (%v) :== \n ", px.me, px.prepareState)


			px.DoneFromPeer(id, reply.DoneNumber)

			if px.prepareState[key] >= ((len(px.peers)/2)+1) {
				DPrintf("\n\n ==: (%v) GotMajorty for %v ", px.me, VaMax)
				go px.Accept(args.N, VaMax, seq)
				
				break
			}
				
		}else{
			px.DoneFromPeer(id, reply.DoneNumber)
		}
	}
}

/*** PAXOS ALGORITHM for single instance 



proposer(v):
  while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
      send accept(n, v') to all
      if accept_ok(n) from majority:
        send decided(v') to all

acceptor's state:
  n_p (highest prepare seen)
  n_a, v_a (highest accept seen)

acceptor's prepare(n) handler:
  if n > n_p
    n_p = n
    reply prepare_ok(n_a, v_a)
  else
    reply prepare_reject

acceptor's accept(n, v) handler:
  if n >= n_p
    n_p = n
    n_a = n
    v_a = v
    reply accept_ok(n)
  else
    reply accept_reject

***/


//
// the application wants paxos to start agreement on
// instance seq, withproposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {

	go px.Prepare(seq, v)

}

func (px *Paxos) DoneFromPeer(peer int,seq int){
	val, ok := px.doneMap[peer]
	if ok {
		if seq < val || val == -1 {
			px.doneMap[peer] = seq
		}
	}else{
		px.doneMap[peer] = seq
	}

	DPrintf("{%v} DoneMap:%v", px.me, px.doneMap)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.doneMap[px.me] = seq
	DPrintf("\n {%v} setting min:%v %v", px.me, px.doneMap[px.me], px.doneMap)
/*
	if(px.min_seq == -1){
		if(px.DoneSeen == false){
			DPrintf("\n {%v} setting min_seq:%v to seq:%v", px.me, px.min_seq, seq)
			px.min_seq = seq;
		}
	}else{
		if(seq < px.min_seq){
			px.min_seq = seq
		}
	}

	px.DoneSeen = true
	DPrintf("\n {%v} Done(%v) min:%v max:%v DoneSeen:%v", px.me, seq, px.min_seq, px.max_seq, px.DoneSeen)
*/
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.max_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	min_val := px.doneMap[px.me]


	for id,v := range px.doneMap {
		//DPrintf("\n [%v][%v] = %v", px.me, k, v)
		if id != px.me && v < min_val {
			min_val = v
		}
	}

	DPrintf("\n {%v} Min() min:%v max:%v %v", px.me, min_val+1, px.max_seq, px.doneMap)

	return min_val + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	if seq <= px.max_seq {
		val := px.decidedState[px.max_seq]
		return val.value.status, val.value.value
	}

	if val, ok := px.decidedState[seq]; ok {
		return val.value.status, val.value.value
	}

	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.n_a = -1
	px.n_p = -1
	px.prepareState = make(map[PrepareKey]int)
	px.acceptState = make(map[AcceptKey]int)
	px.decidedState = make(map[int]Item)
	px.doneMap = make(map[int]int)
	px.state = &PriorityQueue{}


	px.doneMap[px.me] = -1
	px.DoneSeen = false 

	heap.Init(px.state)

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
