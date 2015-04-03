package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMapper(map_count int, w *WorkerInfo){
			args := &DoJobArgs{}
			args.JobNumber = map_count
			args.File = mr.file
			args.Operation = Map
			args.NumOtherPhase = mr.nReduce

			var res DoJobReply
			retries := 0
			for retries < 3 {
				ok := call(w.address, "Worker.DoJob", args, &res)
				if ok == false{
					fmt.Printf("DoJob: Map Failed for worker %s %v \n", w.address)
				}else{
					//fmt.Printf("DoJob: Map Succeeded for worker %s \n", w.address)
					break
				}
				retries += 1
				fmt.Printf("Retryin ... %d\n", retries)
			}
	mr.MapChannel <- true
}

func (mr *MapReduce) RunReducer(reduce_count int, w *WorkerInfo){
			args := &DoJobArgs{}
			args.JobNumber = reduce_count
			args.File = mr.file
			args.Operation = Reduce
			args.NumOtherPhase = mr.nMap

			var res DoJobReply
			retries := 0
			for retries < 3 {
				ok := call(w.address, "Worker.DoJob", args, &res)
				if ok == false {
					fmt.Printf("DoJob: Reduce Failed for worker %s \n", w.address)
				}else{
					//fmt.Printf("DoJob: Reduce succeeded for worker %s \n", w.address)
					break
				}
				retries += 1
				fmt.Printf("Retryin ... %d\n", retries)
			}
	mr.ReduceChannel <- true
}

func (mr *MapReduce) RunMaster() *list.List {
	fmt.Printf("Starting Master with Maps:%i Reducers:%i \n", mr.nMap, mr.nReduce)

	<- mr.registerChannel
	<- mr.registerChannel
	
	map_count := 0
	for map_count < mr.nMap {
		for _, w := range mr.Workers {
			go mr.RunMapper(map_count, w)
			map_count += 1

		}
	}

	map_done := 0
	for map_done < mr.nMap {
		<- mr.MapChannel

		map_done += 1
	}

	reduce_count := 0
	for reduce_count < nReduce {
		for _, w := range mr.Workers {
			go mr.RunReducer(reduce_count, w)
			reduce_count += 1
		}
	}

	reduce_done := 0
	for reduce_done < mr.nReduce {
		<- mr.ReduceChannel

		reduce_done += 1
	}

	return mr.KillWorkers()
}
