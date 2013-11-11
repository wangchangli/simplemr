package edu.cmu.courses.simplemr.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DFSMasterService extends Remote {
    public void heartbeat(String host, int dataPort, int chunkNumber) throws RemoteException;
    public DFSFile createFile(String fileName, int replicas) throws RemoteException;
    public DFSFile getFile(String fileName) throws RemoteException;
    public DFSFile[] listFiles() throws RemoteException;
    public DFSChunk createChunk(long fileId, long offset, long size) throws RemoteException;
    public void deleteFile(long fileId) throws RemoteException;
}
