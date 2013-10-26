namespace java edu.cmu.courses.simplemr.thrift

struct ServiceAddress{
    1: string host
    2: i16 port
}

struct ClientChunk{
    1: i64 id
    2: i64 offset
    3: i64 length
    4: list<ServiceAddress> locations
}

struct ClientFile{
    1: i64 id
    2: string name
    3: list<ClientChunk> chunks
}

struct SlaveWorkload{

}

exception FileNotExistException{
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
    void slaveHeartbeat(1: i64 slaveId, 2: SlaveWorkload workload)
        throws (1: SlaveOperationException soe)

    /**
     * The RPC interface for distributed file system
     */
    ClientFile dfsOpen(1: string name, 2: i16 mode)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    ClientChunk dfsCreateChunk(1: i64 fileId)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    void dfsClose(1: i64 fileId)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    ClientFile dfsGetFile(1: string name)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    ClientChunk dfsGetChunk(1: i64 fileId, 2: i64 index)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    void dfsDelete(1: i64 fileId)
        throws (1: FileNotExistException fne, 2: UnknownException une)
    ClientFile dfsRename(1: i64 fileId, 2: string newName)
        throws (1: FileNotExistException fne, 2: UnknownException une)
}

service SlaveService{
    void dfsLockChunk(1: i64 chunkId)
    void dfsUnlockChunk(1: i64 chunkId)
}