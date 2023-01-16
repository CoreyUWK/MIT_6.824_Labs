# MIT_6.824_Labs
Going through MIT 6.824 Distributed Systems Course Labs.

Lab1: MapReduce
- code files in /mr directory

1 coordinator runs <- RPC -> many workers
- each worker will request a task from coordinator
- coordinator handles each task request on a seperate thread
- A monitor thread started per task request thread to check elapsed time of worker operation, if dies
- use channels as a locked queue for inter-thread communication of tasks

Map Tasks:
- will send file path to worker which will perform map operation
- 1:N files created

MapTasks     ReduceTasks
File1   |         
File2   | -- Reduce File 2 --> Out1,...,OutN  
File3   |
      Barrier
   Wait for all maptasks to complete

Reduce Tasks:
- partitions map results based on hash of key
- each reducer will work on key space
  
                  ReqTask
            Thread  |--- worker
coordinator ...   <-|    ...
            Thread  |----worker
