package edu.cmu.courses.simplemr.dfs.master;

import edu.cmu.courses.simplemr.dfs.DFSChunk;
import edu.cmu.courses.simplemr.dfs.DFSFile;
import edu.cmu.courses.simplemr.dfs.DFSMasterService;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DFSMasterServiceImpl extends UnicastRemoteObject implements DFSMasterService {

    protected DFSMasterServiceImpl() throws RemoteException {

    }

    @Override
    public void heartbeat(String host, int port, int chunkNumber) throws RemoteException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DFSFile createFile(String fileName, int replicas) throws RemoteException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DFSFile getFile(String fileName) throws RemoteException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DFSFile[] listFiles() throws RemoteException {
        return new DFSFile[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DFSChunk createChunk(long fileId, long offset, long size) throws RemoteException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteFile(long fileId) throws RemoteException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
