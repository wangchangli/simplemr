package edu.cmu.courses.simplemr.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

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

    public String[] listFiles(String fileName)
            throws RemoteException{
        DFSFile[] files = masterService.listFiles();
        String[] fileNames = new String[files.length];
        for(int i = 0; i < files.length; i++){
            fileNames[i] = files[i].getName();
        }
        return fileNames;
    }

    public byte[] read(String fileName, int offset, int size)
            throws RemoteException {
        DFSFile file = getFile(fileName);
        if(file == null){
            return null;
        }
        List<Byte> data = new ArrayList<Byte>();
        for(DFSChunk chunk : file.getChunks()){
            if((offset >= chunk.getOffset() && offset < chunk.getOffset() + chunk.getSize()) ||
               (offset + size > chunk.getOffset() && offset <= chunk.getOffset() + chunk.getSize())){
                int chunkOffset = Math.max(offset, chunk.getOffset());
                int chunkSize = Math.min(offset + size, chunk.getOffset() + chunk.getSize()) - chunkOffset;
                byte[] chunkData = readChunk(chunk, chunkOffset, chunkSize);
                if(chunkData == null){
                    break;
                }
                for(byte b : chunkData){
                    data.add(b);
                }
            }
            if(offset + size <= chunk.getOffset()){
                break;
            }
        }
        if(data.size() == 0){
            return null;
        }
        byte[] dataArray = new byte[data.size()];
        for(int i = 0; i < data.size(); i++){
            dataArray[i] = data.get(i);
        }
        return dataArray;
    }

    public boolean write(String fileName, int replicas, int chunkSize)
            throws IOException {
        FileInputStream reader = new FileInputStream(new File(fileName));
        DFSFile file = createFile(fileName, replicas);
        int offset = 0;
        boolean success = true;
        byte[] data = new byte[chunkSize];
        while(true){
            int len = reader.read(data);
            if(len > 0){
                DFSChunk chunk = createChunk(file.getId(), offset, len);
                if(!(success = writeChunk(chunk, 0, len, data))){
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
        int offset = 0;
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

    private DFSFile createFile(String fileName, int replicas)
            throws RemoteException {
        return masterService.createFile(fileName, replicas);
    }

    private DFSFile getFile(String fileName)
            throws RemoteException{
        return masterService.getFile(fileName);
    }

    private void deleteFile(long fileId)
            throws RemoteException{
        masterService.deleteFile(fileId);
    }

    private DFSChunk createChunk(long fileId, int offset, int size)
            throws RemoteException{
        return masterService.createChunk(fileId, offset, size);
    }

    private byte[] readChunk(DFSChunk chunk, int offset, int size){
        DFSNode[] nodes = chunk.getNodes();
        for(DFSNode node : nodes){
            Exception e;
            try{
                return readChunk(node, chunk.getId(), offset, size);
            } catch (Exception exp){
                e = exp;
            }
            LOG.warn("can't read data from node " + node.getServiceName() + ", try next replica", e);
        }
        LOG.error("can't read data from any replica");
        return null;
    }

    private boolean writeChunk(DFSChunk chunk, int offset, int size, byte[] data){
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

    private byte[] readChunk(DFSNode dataNode, long chunkId, int offset, int size)
            throws RemoteException, NotBoundException {
        DFSSlaveService slaveService = (DFSSlaveService) registry.lookup(dataNode.getServiceName());
        return slaveService.read(chunkId, offset, size);
    }

    private boolean writeChunk(DFSNode dataNode, long chunkId, int offset, int size, byte[] data)
            throws RemoteException, NotBoundException {
        DFSSlaveService slaveService = (DFSSlaveService) registry.lookup(dataNode.getServiceName());
        return slaveService.write(chunkId, offset, size, data);
    }
}
