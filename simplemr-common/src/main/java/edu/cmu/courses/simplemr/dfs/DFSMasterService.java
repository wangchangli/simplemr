package edu.cmu.courses.simplemr.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The service interface that a DFS master can provide to DFS client.
 * It extends the Remote interface to be called by RMI.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public interface DFSMasterService extends Remote {
    public void heartbeat(String serviceName, int chunkNumber) throws RemoteException;
    public DFSFile createFile(String fileName, int replicas) throws RemoteException;
    public DFSFile getFile(String fileName) throws RemoteException;
    public DFSFile[] listFiles() throws RemoteException;
    public DFSChunk createChunk(long fileId, long offset, int size) throws RemoteException;
    public void deleteFile(long fileId) throws RemoteException;
}
