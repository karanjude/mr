package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	IsPrimary bool
	CurrentView viewservice.View
	Viewnum uint
	GuidMap map[string]string
	store map[string]string
	lock chan int
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	value, ok := pb.store[args.Key]

	if ok == true {
		reply.Value = value
		reply.Err = OK
	}else{
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	return nil
}


func (pb *PBServer) Replicate(args *ReplicateArgs, reply *ReplicateReply) error {
	reply.Store = make(map[string]string)
	for k,v := range pb.store {
		reply.Store[k] = v
	}

	reply.GuidMap = make(map[string]string)
	for k1,v1 := range pb.GuidMap {
		reply.GuidMap[k1] = v1
	}
	return nil
}

func (pb *PBServer) ReplicateFromPrimary() {
	primary := pb.vs.Primary()

//	fmt.Println("COPYING FROM PRIMARY :", primary)

	if(primary != ""){
		args := &ReplicateArgs{}
		args.Key = "ALL"

		var reply ReplicateReply

		call(primary, "PBServer.Replicate", args, &reply)

		for key, value := range reply.Store {
//			fmt.Println("COPIED Key:", key, "Value:", value)
			pb.store[key] = value
		}	

		for key1, value1 := range reply.GuidMap {
//			fmt.Println("COPIED Key:", key, "Value:", value)
			pb.GuidMap[key1] = value1
		}
	}
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {


	guid, seen_key := pb.GuidMap[args.Key]
	if seen_key == true && guid == args.Guid {
		reply.Err = OK
		return nil
	}

	//fmt.Println("about to append " + args.Value)
	pb.lock <- 1
	_, ok := pb.store[args.Key]

	if ((ok == false) || (args.Op == PUT)) {
		pb.store[args.Key] = args.Value

	}else{

		guid1, seen_key1 := pb.GuidMap[args.Key]
		if seen_key1 == true && guid1 == args.Guid {
			reply.Err = OK
			<- pb.lock
			return nil
		}

		pb.store[args.Key] = pb.store[args.Key] + args.Value


		if pb.CurrentView.Primary == pb.me {
			//fmt.Println("Primary: Appended " + args.Value + " for " + args.Key)
			//fmt.Println(pb.store[args.Key])
			DPrintln("Append Primary Done")
		}else{
			//fmt.Println("Secondary: Appended " + args.Value + " for " + args.Key)
			DPrintln("Append Backup Done")
		}


	}

	if pb.IsPrimary {
		backupServer := pb.CurrentView.Backup
		if ( "" != backupServer && pb.CurrentView.Primary != pb.CurrentView.Backup){
			ok = call(backupServer, "PBServer.PutAppend", args, reply)
		}
	}

	//fmt.Println(pb.store[args.Key])

	pb.GuidMap[args.Key] = args.Guid
	reply.Err = OK

	<- pb.lock

/*
	for key, value := range pb.store {
		fmt.Println("PRIMARY:", pb.IsPrimary, "Key:", key, "Value:", value)
	}
	fmt.Println()
*/

	//call(pb.CurrentView.Backup, "PBServer.PutAppend", args, reply)

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	view , ok := pb.vs.Ping(pb.Viewnum)
	if ok != nil {
		return
	}

	DPrintf("\n ->>> Server View : %v", view)
	pb.Viewnum = view.Viewnum

	pb.CurrentView = view

	if view.Primary == pb.me {
		pb.IsPrimary = true
	}else{
		pb.IsPrimary = false
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)


	pb.store = make(map[string]string)
	pb.GuidMap = make(map[string]string)
	pb.lock = make(chan int, 1)

	// Your pb.* initializations here.
	
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	DPrintf("\n about to ping view service")

	//pb.Viewnum = 0
	//view, _ := pb.vs.Ping(pb.Viewnum)
	//pb.Viewnum = view.Viewnum


	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	pb.ReplicateFromPrimary()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
