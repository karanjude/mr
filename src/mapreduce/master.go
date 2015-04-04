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
			fmt.Printf("DoWork: RPC shutdown error\n")
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
			for retries < 1 {
				ok := call(w.address, "Worker.DoJob", args, &res)
				if ok == false{
				      	 //fmt.Printf("Total workers: %d", len(mr.Workers))
				      	 fmt.Printf("map job failed for %d \n", map_count)
					//fmt.Printf("DoJob: Map Failed for worker %s %v \n", w.address)
					mr.MapWorkQueue <- map_count
				}else{
					//fmt.Printf("DoJob: Map Succeeded for worker %s \n", w.address)
					mr.MapChannel <- map_count		
					break
				}
				retries += 1
				//fmt.Printf("Retryin ... %d\n", retries)
			}

}

func (mr *MapReduce) RunReducer(reduce_count int, w *WorkerInfo){
			args := &DoJobArgs{}
			args.JobNumber = reduce_count
			args.File = mr.file
			args.Operation = Reduce
			args.NumOtherPhase = mr.nMap

			var res DoJobReply
			retries := 0
			for retries < 1 {
				ok := call(w.address, "Worker.DoJob", args, &res)
				if ok == false {
				      	 //fmt.Printf("Total workers: %d", len(mr.Workers))
				      	 fmt.Printf("reduce job failed for %d \n", reduce_count)
					//fmt.Printf("DoJob: Map Failed for worker %s %v \n", w.address)
					mr.ReduceWorkQueue <- reduce_count
				}else{
					//fmt.Printf("DoJob: Reduce succeeded for worker %s \n", w.address)
					mr.ReduceChannel <- reduce_count		
					break
				}
				retries += 1
				//fmt.Printf("Retryin ... %d\n", retries)
			}
}

func (mr *MapReduce) WorkerCoordinator(){
     for {
     	 select {
	  case worker := <-mr.registerChannel:
	  	    fmt.Printf("Added worker: %s \n", worker)
	  default:
	 }
    }
}


func (mr *MapReduce) RunMaster() *list.List {
	fmt.Printf("Starting Master with Maps:%i Reducers:%i \n", mr.nMap, mr.nReduce)

	go mr.WorkerCoordinator()

	map_count := 0
	for map_count < mr.nMap {
	    mr.MapWorkQueue <- map_count
	    map_count += 1
	}

	map_done := 0
	for map_done < mr.nMap {
		fmt.Printf("Map Job Completed for %d\n", <- mr.MapChannel)
		fmt.Printf("MAP COMPLETE ========================================> %d / %d\n", map_done+1, nMap)
		map_done += 1
	}

	close(mr.MapWorkQueue)

	fmt.Printf("Submitted Map jobs:%d, completed jobs:%d\n", nMap, map_done)

	reduce_count := 0
	for reduce_count < nReduce {
	    mr.ReduceWorkQueue <- reduce_count
	    reduce_count += 1
	}

	reduce_done := 0
	for reduce_done < mr.nReduce {
		fmt.Printf("Reduce Job Completed for %d\n", <- mr.ReduceChannel)
		reduce_done += 1
		fmt.Printf("Reduce COMPLETE ========================================> %d\n", (reduce_done / mr.nReduce) * 100)
	}

	close(mr.ReduceWorkQueue)

	fmt.Printf("Submitted Reduce jobs:%d, completed jobs:%d\n", nReduce, reduce_done)

	return mr.KillWorkers()
}
