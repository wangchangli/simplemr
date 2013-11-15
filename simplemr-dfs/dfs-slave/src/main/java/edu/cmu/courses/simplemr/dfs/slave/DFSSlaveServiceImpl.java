package edu.cmu.courses.simplemr.dfs.slave;

import edu.cmu.courses.simplemr.dfs.DFSSlaveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DFSSlaveServiceImpl extends UnicastRemoteObject implements DFSSlaveService {

    private static Logger LOG = LoggerFactory.getLogger(DFSSlaveServiceImpl.class);

    private DFSSlave slave;

    protected DFSSlaveServiceImpl(DFSSlave slave) throws RemoteException {
        this.slave = slave;
    }

    @Override
    public byte[] read(long chunkId, long offset, int size) throws RemoteException {
        try {
            return slave.read(chunkId, offset, size);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean write(long chunkId, long offset, int size, byte[] data) throws RemoteException {
        try {
            slave.write(chunkId, offset, size, data);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void delete(long chunkId) throws RemoteException {
        LOG.info("delete chunk " + chunkId);
        slave.delete(chunkId);
    }

    @Override
    public long[] linesOffset(long chunkId) throws RemoteException {
        try {
            return slave.linesOffset(chunkId);
        } catch (IOException e) {
            throw new RemoteException("can't access chunk " + chunkId);
        }
    }
}
