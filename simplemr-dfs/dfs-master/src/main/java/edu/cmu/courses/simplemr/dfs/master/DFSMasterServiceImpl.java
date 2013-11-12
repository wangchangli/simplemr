package edu.cmu.courses.simplemr.dfs.master;

import edu.cmu.courses.simplemr.dfs.DFSChunk;
import edu.cmu.courses.simplemr.dfs.DFSFile;
import edu.cmu.courses.simplemr.dfs.DFSMasterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DFSMasterServiceImpl extends UnicastRemoteObject implements DFSMasterService {

    private static Logger LOG = LoggerFactory.getLogger(DFSMasterServiceImpl.class);

    private DFSMetaData metaData;

    protected DFSMasterServiceImpl(DFSMetaData metaData) throws RemoteException {
        super();
        this.metaData = metaData;
    }

    @Override
    public void heartbeat(String serviceName, int chunkNumber) throws RemoteException {
        LOG.info("heartbeat from " + serviceName + ", chunk number: " + chunkNumber);
        metaData.updateDataNode(serviceName, chunkNumber, true);
    }

    @Override
    public DFSFile createFile(String fileName, int replicas) throws RemoteException {
        LOG.info("create file " + fileName + ", replica number " + replicas);
        return metaData.createFile(fileName, replicas, true);
    }

    @Override
    public DFSFile getFile(String fileName) throws RemoteException {
        LOG.info("get file " + fileName);
        return metaData.getFile(fileName);
    }

    @Override
    public DFSFile[] listFiles() throws RemoteException {
        LOG.info("list files");
        return metaData.listFiles();
    }

    @Override
    public DFSChunk createChunk(long fileId, int offset, int size) throws RemoteException {
        LOG.info("create chunk for file " + fileId + ", offset " + offset + ", size " + size);
        return metaData.createChunk(fileId, offset, size, true);
    }

    @Override
    public void deleteFile(long fileId) throws RemoteException {
        LOG.info("delete file " + fileId);
        metaData.deleteFile(fileId, true);
    }
}
