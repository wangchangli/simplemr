package edu.cmu.courses.simplemr.dfs.slave;

import edu.cmu.courses.simplemr.dfs.DFSSlaveService;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DFSSlaveServiceImpl extends UnicastRemoteObject implements DFSSlaveService {

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
}
