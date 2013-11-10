package edu.cmu.courses.simplemr.master;

import edu.cmu.courses.simplemr.thrift.*;
import org.apache.thrift.TException;

import java.util.List;

public class MasterServiceHandler implements edu.cmu.courses.simplemr.thrift.MasterService.Iface {
    @Override
    public long slaveRegistry(ServiceAddress location, List<Long> holdingChunks, SlaveWorkload workload) throws SlaveOperationException, TException {
        return 0;  //TODO needs implementation.
    }

    @Override
    public DFSFile dfsCreateFile(String name, int replicas) throws FileAlreadyExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public DFSChunk dfsCreateChunk(long fileId, long offset, long length) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public DFSFile dfsGetFile(String name) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public DFSChunk dfsGetChunk(long chunkId) throws UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public void dfsDelete(long fileId) throws FileNotExistException, UnknownException, TException {
        //TODO needs implementation.
    }
}
