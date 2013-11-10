package edu.cmu.courses.simplemr.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.cmu.courses.simplemr.Slave;
import edu.cmu.courses.simplemr.thrift.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MetaData {
    private static long fileId = 0;
    private static long chunkId = 0;
    private static long slaveId = 0;

    private Map<String, Long> fileNames;
    private Map<Long, DFSFile> files;
    private Map<Long, DFSChunk> chunks;
    private Map<String, Slave> slaves;
    private EditLogger editLogger;

    public MetaData(EditLogger logger){
        fileNames = new HashMap<String, Long>();
        files = new HashMap<Long, DFSFile>();
        chunks = new HashMap<Long, DFSChunk>();
        slaves = new HashMap<String, Slave>();
        editLogger = logger;
    }

    public long registerSlave(ServiceAddress location, List<Long> holdingChunks, SlaveWorkload workload)
            throws IOException {
        dispatchLog(EditOperation.SLAVE_REGISTER, new Object[] {location.getHost(), location.getPort(),
                holdingChunks, workload});
        String id = Slave.locationToString(location);
        Slave slave = slaves.get(id);
        if(slave == null){
            slave = new Slave(slaveId++, location, workload);
            slaves.put(id, slave);
        } else {
            slave.setWorkload(workload);
        }
        addChunkLocation(location, holdingChunks);
        return slave.getId();
    }

    public void removeSlave(long slaveId)
            throws IOException {
        dispatchLog(EditOperation.SLAVE_REMOVE, new Object[]{slaveId});
    }

    public DFSFile dfsCreateFile(String name, int replicas)
            throws FileAlreadyExistException, IOException{
        if(fileNames.containsKey(name)){
            throw new FileAlreadyExistException("file " + name +" is already exist!");
        }
        dispatchLog(EditOperation.DFS_DELETE_FILE, new Object[]{replicas});
        DFSFile dfsFile = new DFSFile(fileId++, name, new ArrayList<Long>(), replicas);
        fileNames.put(name, fileId);
        files.put(fileId, dfsFile);
        return dfsFile;
    }

    public void dfsDeleteFile(long fileId)
            throws FileNotExistException, IOException {
        if(!files.containsKey(fileId)){
            throw new FileNotExistException("file id " + fileId + " is not exist!");
        }
        dispatchLog(EditOperation.DFS_DELETE_FILE, new Object[]{fileId});
        DFSFile file = files.remove(fileId);
        for(Long chunkId: file.chunkIds){
            chunks.remove(chunkId);
        }
        fileNames.remove(file.name);
    }

    public DFSChunk dfsCreateChunk(long fileId, long offset, long length, ServiceAddress[] addresses)
            throws FileNotExistException, IOException {
        if(!files.containsKey(fileId)){
            throw new FileNotExistException("file id " + fileId + " is not exist!");
        }
        dispatchLog(EditOperation.DFS_CREATE_CHUNK, new Object[]{fileId, offset, length, addresses});
        DFSFile file = files.get(fileId);
        DFSChunk chunk = new DFSChunk(chunkId++, fileId, offset, length, Arrays.asList(addresses));
        file.chunkIds.add(chunk.getId());
        chunks.put(chunk.getId(), chunk);
        return chunk;
    }

    public Slave[] getSlaves(){
        Slave[] slaveArray = new Slave[slaves.size()];
        slaves.values().toArray(slaveArray);
        return slaveArray;
    }

    public DFSFile dfsGetFile(long fileId)
            throws FileNotExistException {
        if(!files.containsKey(fileId)){
            throw new FileNotExistException("file id " + fileId + " is not exist!");
        }
        return files.get(fileId);
    }

    public DFSChunk dfsGetChunk(long chunkId){
        return chunks.get(chunkId);
    }

    public void recoveryFromLogs()
            throws FileNotFoundException, IOException, FileAlreadyExistException, FileNotExistException {
        editLogger.disable();
        String path = editLogger.getPath();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = null;
        ObjectMapper mapper = new ObjectMapper();
        while((line = reader.readLine()) != null){
            EditOperation operation = mapper.readValue(line, EditOperation.class);
            dispatchOperation(operation);
        }
        reader.close();
        editLogger.enable();
    }

    private void addChunkLocation(ServiceAddress location, List<Long> chunkIds){
        for(long chunkId : chunkIds){
            DFSChunk chunk = chunks.get(chunkId);
            boolean contains = false;
            if(chunk != null && !chunkContainsLocation(chunk, location)){
                chunk.getLocations().add(location);
            }
        }
    }

    private boolean chunkContainsLocation(DFSChunk chunk, ServiceAddress location){
        for(ServiceAddress chunkLocation : chunk.getLocations()){
            if(Slave.locationToString(location).equals(Slave.locationToString(chunkLocation))){
                return true;
            }
        }
        return false;
    }

    private void dispatchLog(byte type, Object[] arguments) throws IOException {
        EditOperation operation = new EditOperation(type, arguments);
        editLogger.addLog(operation);
    }

    private void dispatchOperation(EditOperation operation)
            throws IOException, FileAlreadyExistException, FileNotExistException {
        switch (operation.getType()){
            case EditOperation.SLAVE_REGISTER:
                registerSlave(operation);
                break;
            case EditOperation.SLAVE_REMOVE:
                removeSlave(operation);
                break;
            case EditOperation.DFS_CREATE_FILE:
                dfsCreateFile(operation);
                break;
            case EditOperation.DFS_DELETE_FILE:
                dfsDeleteFile(operation);
                break;
            case EditOperation.DFS_CREATE_CHUNK:
                dfsCreateChunk(operation);
                break;
            default:
                return;
        }

    }

    private void registerSlave(EditOperation operation)
            throws IOException {
        Object[] arguments = operation.getArguments();
        registerSlave(
                mapToLocation((Map<String, Object>)arguments[0]),
                (List<Long>)arguments[1],
                mapToWorkload((Map<String, Object>)arguments[2])
        );

    }

    private void removeSlave(EditOperation operation)
            throws IOException {
        Object[] arguments = operation.getArguments();
        removeSlave((Long)arguments[0]);
    }

    private void dfsCreateFile(EditOperation operation)
            throws FileAlreadyExistException, IOException {
        Object[] arguments = operation.getArguments();
        dfsCreateFile((String)arguments[0], (Integer)arguments[1]);
    }

    private void dfsDeleteFile(EditOperation operation)
            throws FileNotExistException, IOException {
        Object[] arguments = operation.getArguments();
        dfsDeleteFile((Long)arguments[0]);
    }

    private void dfsCreateChunk(EditOperation operation)
            throws FileNotExistException, IOException {
        Object[] arguments = operation.getArguments();
        ServiceAddress[] addresses = new ServiceAddress[((List<Object>)arguments[3]).size()];
        for(int i = 0; i < addresses.length; i++){
            addresses[i] = mapToLocation((Map<String, Object>)((List<Object>)arguments[3]).get(i));
        }
        dfsCreateChunk((Long)arguments[0], (Long)arguments[1], (Long)arguments[2], addresses);
    }

    private ServiceAddress mapToLocation(Map<String, Object> map){
        ServiceAddress location = new ServiceAddress();
        location.setHost((String)map.get("host"));
        location.setPort((Short)map.get("port"));
        return location;
    }

    private SlaveWorkload mapToWorkload(Map<String, Object> map){
        SlaveWorkload workload = new SlaveWorkload();
        workload.setRunningTasks((Long)map.get("runningTasks"));
        return workload;
    }
}
