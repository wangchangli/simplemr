package edu.cmu.courses.simplemr.slave;

import edu.cmu.courses.simplemr.thrift.SlaveService;
import org.apache.thrift.TException;

public class SlaveServiceHandler implements SlaveService.Iface{
    @Override
    public void dfsLockChunk(long chunkId) throws TException {
        //TODO needs implementation.
    }

    @Override
    public void dfsUnlockChunk(long chunkId) throws TException {
        //TODO needs implementation.
    }
}
