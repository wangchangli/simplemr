package edu.cmu.courses.simplemr.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DFSSlaveService extends Remote {
    public byte[] read(long chunkId, long offset, int size) throws RemoteException;
    public boolean write(long chunkId, long offset, int size, byte[] data) throws RemoteException;
    public void delete(long chunkId) throws RemoteException;
}
