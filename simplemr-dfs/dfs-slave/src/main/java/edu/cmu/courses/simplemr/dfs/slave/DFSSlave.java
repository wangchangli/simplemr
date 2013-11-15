package edu.cmu.courses.simplemr.dfs.slave;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.dfs.DFSConstants;
import edu.cmu.courses.simplemr.dfs.DFSMasterService;
import edu.cmu.courses.simplemr.dfs.DFSSlaveService;
import org.apache.commons.lang.ArrayUtils;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DFSSlave {

    @Parameter(names = {"-rh", "--registry-host"}, description = "The host of registry service")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "The port of registry service")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-n", "--name"}, description = "The name of slave node", required = true)
    private String serviceName;

    @Parameter(names = {"-b", "--heartbeat"}, description = "The period of heartbeat")
    private long heartbeatPeriod = Constants.DEFAULT_HEARTBEAT_PERIOD;

    @Parameter(names = {"-d", "--data"}, description = "The path of data")
    private String dataDir = DFSConstants.DEFAULT_SLAVE_DATA_PATH;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private DFSSlaveService slaveService;
    private DFSMasterService masterService;
    private Registry registry;
    private ScheduledExecutorService heartbeatService;

    public void start()
            throws RemoteException, NotBoundException {
        dataDir = dataDir + System.getProperty("file.separator") + serviceName;
        File dataDirFile = new File(dataDir);
        if(!dataDirFile.exists()){
            dataDirFile.mkdirs();
        }
        slaveService = new DFSSlaveServiceImpl(this);
        registry = LocateRegistry.getRegistry(registryHost, registryPort);
        registry.rebind(serviceName, slaveService);
        masterService = (DFSMasterService) registry.lookup(DFSMasterService.class.getCanonicalName());
        heartbeatService = Executors.newScheduledThreadPool(Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
        heartbeatService.scheduleAtFixedRate(new DFSSlaveHeartbeatWorker(this, registry),
                0, heartbeatPeriod, TimeUnit.MILLISECONDS);
    }

    public byte[] read(long chunkId, long offset, int size)
            throws IOException {
        File file = new File(getFilePath(chunkId));
        if(file.exists()){
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            byte[] buffer = new byte[size];
            int len = raf.read(buffer, 0, size);
            if(len > 0){
                byte[] data = Arrays.copyOf(buffer, len);
                raf.close();
                return data;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public void write(long chunkId, long offset, int size, byte[] data)
            throws IOException{
        File file = new File(getFilePath(chunkId));
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(offset);
        raf.write(data, 0, Math.min(size, data.length));
        raf.close();
    }

    public long[] linesOffset(long chunkId)
            throws IOException {
        File file = new File(getFilePath(chunkId));
        List<Long> offsets = new ArrayList<Long>();
        FileInputStream in = new FileInputStream(file);
        byte ch;
        byte prevChar = -1;
        long offset = 0;
        while((ch = (byte)in.read()) != -1){
            if(prevChar == -1 || prevChar == '\n'){
                offsets.add(offset);
            }
            offset++;
            prevChar = ch;
        }
        Long[] results = new Long[offsets.size()];
        offsets.toArray(results);
        return ArrayUtils.toPrimitive(results);
    }

    public void delete(long chunkId){
        File file = new File(getFilePath(chunkId));
        if(file.exists()){
            file.delete();
        }
    }

    public String getServiceName(){
        return serviceName;
    }

    public int getChunkNumber(){
        File file = new File(dataDir);
        return file.listFiles().length;
    }

    public boolean needHelp(){
        return help;
    }

    private String getFilePath(long chunkId){
        return dataDir + System.getProperty("file.separator") + DFSConstants.CHUNK_PREFIX + chunkId;
    }

    public static void main(String[] args) {
        DFSSlave slave = new DFSSlave();
        JCommander commander = new JCommander(slave, args);
        commander.setProgramName("dfs-slave");
        if(slave.needHelp()){
            commander.usage();
        } else {
            try {
                slave.start();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

}
