namespace java edu.cmu.courses.simplemr.thrift

struct ServiceAddress{
    1: string host
    2: i16 port
}

struct DFSChunk{
    1: i64 id
    2: i64 fileId
    3: i64 offset
    4: i64 length
    5: list<ServiceAddress> locations
}

struct DFSFile{
    1: i64 id
    2: string name
    3: list<i64> chunkIds
    4: i32 replicas;
}

struct SlaveWorkload{
    1: i64 runningTasks
}

exception FileNotExistException{
    1: string message
}

exception FileAlreadyExistException{
    1: string message
}

exception UnknownException{
    1: string message
}

exception SlaveOperationException{
    1: string message
}

service MasterService{
    /**
     * The RPC interface for slave
     */
    i64 slaveRegistry(1: ServiceAddress location, 2: list<i64> holdingChunks, 3: SlaveWorkload workload)
        throws (1: SlaveOperationException soe)

    /**
     * The RPC interface for distributed file system
     */
    DFSFile dfsCreateFile(1: string name, 2: i32 replicas)
        throws (1: FileAlreadyExistException fne, 2: UnknownException une)
    DFSChunk dfsCreateChunk(1: i64 fileId, 2: i64 offset, 3: i64 length)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    DFSFile dfsGetFile(1: string name)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    DFSChunk dfsGetChunk(1: i64 chunkId)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    void dfsDelete(1: i64 fileId)
        throws (1: UnknownException une)
}

service SlaveService{
    SlaveWorkload heartbeat()
}