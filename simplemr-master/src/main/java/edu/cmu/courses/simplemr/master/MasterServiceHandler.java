package edu.cmu.courses.simplemr.master;

import edu.cmu.courses.simplemr.thrift.*;
import org.apache.thrift.TException;

import java.util.List;

public class MasterServiceHandler implements MasterService.Iface {
    @Override
    public long slaveRegistry(ServiceAddress location, List<Long> holdingChunks, SlaveWorkload workload) throws SlaveOperationException, TException {
        return 0;  //TODO needs implementation.
    }

    @Override
    public void slaveHeartbeat(long slaveId, SlaveWorkload workload) throws SlaveOperationException, TException {
        //TODO needs implementation.
    }

    @Override
    public ClientFile dfsOpen(String name, short mode) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public ClientChunk dfsCreateChunk(long fileId) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public void dfsClose(long fileId) throws FileNotExistException, UnknownException, TException {
        //TODO needs implementation.
    }

    @Override
    public ClientFile dfsGetFile(String name) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public ClientChunk dfsGetChunk(long fileId, long index) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }

    @Override
    public void dfsDelete(long fileId) throws FileNotExistException, UnknownException, TException {
        //TODO needs implementation.
    }

    @Override
    public ClientFile dfsRename(long fileId, String newName) throws FileNotExistException, UnknownException, TException {
        return null;  //TODO needs implementation.
    }
}
