package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	PrevView *View
	CurrentView *View

	Primary string
	Viewnum uint
	Backup string
	ACK bool

	PrimaryLastPingTime time.Time
	BackupLastPingTime time.Time
	ShouldIncrement bool

	PrimaryExpectedACKViewNum uint
	SlideWindow bool
}

func (vs *ViewServer) MakeView() View {
	if vs.CurrentView == nil {
		vs.CurrentView = &View{}
		vs.CurrentView.Viewnum = vs.Viewnum
		vs.CurrentView.Primary = vs.Primary
		vs.CurrentView.Backup = vs.Backup
		vs.CurrentView.ACK = vs.ACK
	}else if(vs.PrevView != nil && vs.CurrentView.ACK == false && vs.PrevView.ACK == true){
		return *vs.PrevView
	}

	return *vs.CurrentView
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	UpdateView := false

	//fmt.Printf(" \n --> ViewService : %v \n", vs)
	//fmt.Printf("\n (%v, %v, %v) (%v, %v)", vs.Viewnum, vs.Primary, vs.Backup, args.Viewnum, args.Me)

	/*
	if nil != vs.PrevView {
		fmt.Printf("\n Prev--> (%v, %v, %v)", vs.PrevView.Viewnum, vs.PrevView.Primary, vs.PrevView.Backup)
	}*/

	if(args.Viewnum == 0 &&  vs.CurrentView.Primary == "" ){
		
		if vs.PrevView == nil || (vs.PrevView != nil && (vs.PrevView.Primary == args.Me || vs.PrevView.Backup == args.Me)) {
			vs.Primary = args.Me
			vs.Backup = ""
			vs.Viewnum = 1
			vs.PrimaryExpectedACKViewNum = vs.Viewnum
			vs.PrimaryLastPingTime = time.Now()
			vs.ACK  = false
			UpdateView = true
			
		}

	}else if(args.Viewnum == 0 && vs.CurrentView.Backup == "" && vs.CurrentView.Primary != "" && vs.CurrentView.Primary != args.Me){
		vs.Backup = args.Me
		vs.BackupLastPingTime = time.Now()
		vs.SlideWindow = true

	}else if(args.Viewnum == vs.PrimaryExpectedACKViewNum && vs.CurrentView.Primary == args.Me) {
		vs.PrimaryLastPingTime = time.Now()
		
		if(vs.SlideWindow){
			vs.Viewnum = vs.Viewnum + 1
			vs.PrimaryExpectedACKViewNum = vs.Viewnum
			vs.ACK = true
		}

		UpdateView = true
	}else if(args.Viewnum == vs.CurrentView.Viewnum && vs.CurrentView.Primary == args.Me){
		vs.PrimaryLastPingTime = time.Now()
	}else if(args.Viewnum == vs.CurrentView.Viewnum && vs.CurrentView.Backup == args.Me){
		vs.BackupLastPingTime = time.Now()
	}

	if UpdateView {
		vs.PrevView = vs.CurrentView
		
		vs.CurrentView = &View{}
		vs.CurrentView.Primary = vs.Primary
		vs.CurrentView.Backup = vs.Backup
		vs.CurrentView.Viewnum = vs.Viewnum
		vs.CurrentView.ACK = vs.ACK
		
		vs.SlideWindow = false
	}
	
	reply.View = vs.MakeView()

	DPrintf("<-- VIEW SERVER PING REPLY :%v\n\n", reply.View)

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	reply.View = vs.MakeView()

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	t_now := time.Now()
	time_diff := t_now.Sub(vs.PrimaryLastPingTime)

	flipped := false

	if float64(time_diff.Seconds()) > float64(.5) {
		//fmt.Printf("\nPrimary Time diff : %v", time_diff)

		vs.Primary = vs.Backup
		vs.Backup = ""

		//vs.PrimaryLastPingTime = time.Now()
		flipped = true
		
		//fmt.Println("Primary probably dead")
	}

	time_diff = t_now.Sub(vs.BackupLastPingTime)

	if float64(time_diff.Seconds()) > float64(.5) {
		//fmt.Printf("\nBackup Time diff : %v", time_diff)
		vs.Backup = ""
		
		//vs.BackupLastPingTime = time.Now()
		flipped = true

		//fmt.Println("Backup Probably Dead")
	}

	if (flipped){
		
		vs.CurrentView = nil
		vs.SlideWindow = true

		vs.MakeView()

		DPrintf("\n flipped %v", vs.CurrentView)
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.Primary = ""
	vs.Backup = ""
	vs.Viewnum = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
