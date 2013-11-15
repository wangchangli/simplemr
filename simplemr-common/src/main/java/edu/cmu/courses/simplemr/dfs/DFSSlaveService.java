package edu.cmu.courses.simplemr.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The service interface that a DFS slave can provide to DFS master.
 * It extends the Remote interface to be called by RMI.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public interface DFSSlaveService extends Remote {
    public byte[] read(long chunkId, long offset, int size) throws RemoteException;
    public boolean write(long chunkId, long offset, int size, byte[] data) throws RemoteException;
    public void delete(long chunkId) throws RemoteException;
    public long[] linesOffset(long chunkId) throws RemoteException;
}
