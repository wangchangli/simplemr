package edu.cmu.courses.simplemr.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DFSClient {
    private static Logger LOG = LoggerFactory.getLogger(DFSClient.class);

    private DFSMasterService masterService;
    private String registryHost;
    private int registryPort;
    private Registry registry;

    public DFSClient(String registryHost, int registryPort){
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public void connect()
            throws RemoteException, NotBoundException {
        registry = LocateRegistry.getRegistry(registryHost, registryPort);
        masterService = (DFSMasterService)registry.lookup(DFSMasterService.class.getCanonicalName());
    }

    public DFSFile createFile(String fileName, int replicas)
            throws RemoteException {
        File file = new File(fileName);
        return masterService.createFile(file.getName(), replicas);
    }

    public DFSChunk createChunk(long fileId, long offset, int size)
            throws RemoteException{
        return masterService.createChunk(fileId, offset, size);
    }

    public String[] listFiles()
            throws RemoteException{
        DFSFile[] files = masterService.listFiles();
        String[] fileNames = new String[files.length];
        for(int i = 0; i < files.length; i++){
            fileNames[i] = files[i].getName();
        }
        return fileNames;
    }

    public DFSFile getFile(String fileName)
            throws RemoteException{
        File file = new File(fileName);
        return masterService.getFile(file.getName());
    }

    public byte[] readChunk(DFSChunk chunk, long offset, int size){
        DFSNode[] nodes = chunk.getNodes();
        for(DFSNode node : nodes){
            Exception e;
            try{
                return readChunk(node, chunk.getId(), offset, size);
            } catch (Exception exp){
                e = exp;
            }
        }
        LOG.error("can't read data from any replica");
        return null;
    }

    public boolean writeChunk(DFSChunk chunk, long offset, int size, byte[] data){
        DFSNode[] nodes = chunk.getNodes();
        for(DFSNode node : nodes){
            try{
                if(!writeChunk(node, chunk.getId(), offset, size, data)){
                    return false;
                }
            } catch (Exception e){
                LOG.error("can't write data to node " + node.getServiceName(), e);
                return false;
            }
        }
        return true;
    }

    public boolean write(String fileName, int replicas, int chunkSize)
            throws IOException {
        FileInputStream reader = new FileInputStream(new File(fileName));
        DFSFile file = createFile(fileName, replicas);
        long offset = 0;
        boolean success = true;
        byte[] data = new byte[chunkSize];
        while(true){
            int len = reader.read(data);
            if(len > 0){
                DFSChunk chunk = createChunk(file.getId(), offset, len);
                if(chunk == null || !(success = writeChunk(chunk, 0, len, data))){
                    break;
                }
                offset += chunkSize;
            } else {
                break;
            }
        }
        reader.close();
        return success;
    }

    public boolean writeText(String fileName, int replicas, int lineCount)
            throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        DFSFile file = createFile(fileName, replicas);
        long offset = 0;
        int count = 0;
        StringBuffer sb = new StringBuffer();
        while(true){
            String line = reader.readLine();
            count++;
            if((line == null && sb.length() > 0) || count % lineCount == 0){
                if(line != null){
                    sb.append(line + "\n");
                }
                byte[] data = sb.toString().getBytes();
                DFSChunk chunk = createChunk(file.getId(), offset, data.length);
                if(chunk == null || !writeChunk(chunk, 0, data.length, data)){
                    return false;
                }
                offset += data.length;
                sb.delete(0, data.length);
            } else {
                sb.append(line + "\n");
            }
            if(line == null){
                break;
            }

        }
        reader.close();
        return true;
    }

    public void deleteFile(String fileName)
            throws RemoteException{
        File file = new File(fileName);
        DFSFile dfsFile = getFile(file.getName());
        if(dfsFile != null){
            masterService.deleteFile(dfsFile.getId());
        }
    }

    private byte[] readChunk(DFSNode dataNode, long chunkId, long offset, int size)
            throws RemoteException, NotBoundException {
        DFSSlaveService slaveService = (DFSSlaveService) registry.lookup(dataNode.getServiceName());
        return slaveService.read(chunkId, offset, size);
    }

    private boolean writeChunk(DFSNode dataNode, long chunkId, long offset, int size, byte[] data)
            throws RemoteException, NotBoundException {
        DFSSlaveService slaveService = (DFSSlaveService) registry.lookup(dataNode.getServiceName());
        return slaveService.write(chunkId, offset, size, data);
    }
}
