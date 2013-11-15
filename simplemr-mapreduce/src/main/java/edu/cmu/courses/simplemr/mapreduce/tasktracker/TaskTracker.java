package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.mapreduce.common.MapReduceConstants;
import edu.cmu.courses.simplemr.mapreduce.fileserver.FileServer;
import edu.cmu.courses.simplemr.mapreduce.jobtracker.JobTrackerService;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;

public class TaskTracker {

    @Parameter(names = {"-dh", "--dfs-master-registry-host"}, description = "the registry host of DFS master")
    private String dfsMasterRegistryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-dp", "--dfs-master-registry-port"}, description = "the registry port of DFS master")
    private int dfsMasterRegistryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-jh", "--job-tracker-registry-host"}, description = "the registry host of job tracker")
    private String jobTrackerRegistryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-jp", "--job-tracker-registry-port"}, description = "the registry port of job tracker")
    private int jobTrackerRegistryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-rp", "--registry-port"}, description = "the local registry port")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-fp", "--file-server-port"}, description = "the port of file server")
    private int fileServerPort = MapReduceConstants.DEFAULT_FILE_SERVER_PORT;

    @Parameter(names = {"-t", "--temp-dir"}, description = "the directory of temporary files")
    private String tempDir = "/tmp/simplemr-mapreduce-tasktracker";

    @Parameter(names = {"-p", "--invalid-period"}, description = "the period of invalid state")
    private long invalidPeriod = Constants.DEFAULT_HEARTBEAT_INVALID;

    @Parameter(names = {"-b", "--heartbeat"}, description = "the period of heartbeat (ms)")
    private long heartbeatPeriod = Constants.DEFAULT_HEARTBEAT_PERIOD;

    @Parameter(names = {"-n", "--num-threads"}, description = "the number of threads")
    private int threadPoolSize = Constants.DEFAULT_THREAD_POOL_SIZE;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private static Logger LOG = LoggerFactory.getLogger(TaskTracker.class);

    private TaskTrackerInfo taskTrackerInfo;
    private ConcurrentHashMap<Integer, TaskTrackerReducerWorker> reducerWorkers;

    private TaskTrackerService taskTrackerService;
    private JobTrackerService jobTrackerService;
    private ExecutorService threadPool;
    private ScheduledExecutorService heartbeatPool;
    private Registry jobTrackerRegistry;

    public TaskTracker(){
        reducerWorkers = new ConcurrentHashMap<Integer, TaskTrackerReducerWorker>();
    }

    public void start()
            throws Exception {
        taskTrackerInfo = new TaskTrackerInfo(Utils.getHost(), registryPort, fileServerPort, invalidPeriod);
        threadPool = Executors.newFixedThreadPool(threadPoolSize);
        heartbeatPool = Executors.newScheduledThreadPool(Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
        bindService();
        heartbeatPool.scheduleAtFixedRate(new TaskTrackerHeartbeat(this), 0, heartbeatPeriod, TimeUnit.MILLISECONDS);
        new FileServer(fileServerPort, tempDir).start();
    }

    public void runMapperTask(MapperTask task){
        task.setOutputDir(tempDir);
        task.setFileServerHost(taskTrackerInfo.getHost());
        task.setFileServerPort(taskTrackerInfo.getFileServerPort());
        task.createTaskFolder();
        threadPool.execute(new TaskTrackerMapperWorker(task, this));
        taskTrackerInfo.increaseMapperTaskNumber();
    }

    public void runReducerTask(MapperTask mapperTask, List<ReducerTask> reducerTasks){
        for(ReducerTask reducerTask : reducerTasks){
            runReducerTask(mapperTask, reducerTask);
        }
    }

    public void increaseReducerTaskAmount(){
        taskTrackerInfo.increaseReducerTaskNumber();
    }

    public void mapperSucceed(MapperTask task){
        task.setStatus(TaskStatus.SUCCEED);
        try {
            jobTrackerService.mapperTaskSucceed(task);
        } catch (RemoteException e) {
            LOG.warn("can't communicate with job tracker", e);
        }
        taskTrackerInfo.decreaseMapperTaskNumber();
        taskFinished(task);
    }

    public void mapperFailed(MapperTask task){
        try {
            jobTrackerService.mapperTaskFailed(task);
        } catch (RemoteException e) {
            LOG.warn("can't communicate with job tracker", e);
        }
        taskTrackerInfo.decreaseMapperTaskNumber();
        taskFinished(task);
    }

    public void reducerFailedOnMapper(ReducerTask reducerTask, MapperTask mapperTask){
        mapperTask.setStatus(TaskStatus.FAILED);
        try {
            jobTrackerService.reducerTaskFailedOnMapperTask(reducerTask, mapperTask);
        } catch (RemoteException e) {
            LOG.warn("can't communicate with job tracker", e);
        }
    }

    public void reducerFailed(ReducerTask task){
        try {
            jobTrackerService.reducerTaskFailed(task);
        } catch (RemoteException e) {
            LOG.warn("can't communicate with job tracker", e);
        }
        taskTrackerInfo.decreaseReducerTaskNumber();
        taskFinished(task);
    }

    public void reducerSucceed(ReducerTask task){
        task.setStatus(TaskStatus.SUCCEED);
        try {
            jobTrackerService.reducerTaskSucceed(task);
        } catch (RemoteException e) {
            LOG.warn("can't communicate with job tracker", e);
        }
        taskTrackerInfo.decreaseReducerTaskNumber();
        taskFinished(task);
    }

    public void heartbeat(){
        try {
            jobTrackerService.heartbeat(taskTrackerInfo);
        } catch (RemoteException e) {
            LOG.error("can't heartbeat with job tracker", e);
        }
    }

    public String getDfsMasterRegistryHost() {
        return dfsMasterRegistryHost;
    }

    public int getDfsMasterRegistryPort() {
        return dfsMasterRegistryPort;
    }

    public String getJobTrackerRegistryHost() {
        return jobTrackerRegistryHost;
    }

    public int getJobTrackerRegistryPort() {
        return jobTrackerRegistryPort;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public Registry getJobTrackerRegistry(){
        return jobTrackerRegistry;
    }

    public boolean needHelp(){
        return help;
    }

    private void bindService() {
        try{
            taskTrackerService = new TaskTrackerServiceImpl(this);
            Registry registry = LocateRegistry.getRegistry(Utils.getHost(), registryPort);
            registry.rebind(taskTrackerInfo.toString(), taskTrackerService);
            jobTrackerRegistry = LocateRegistry.getRegistry(jobTrackerRegistryHost, jobTrackerRegistryPort);
            jobTrackerService = (JobTrackerService)jobTrackerRegistry.lookup(JobTrackerService.class.getCanonicalName());
        } catch (RemoteException e){
            LOG.error("registry server error", e);
            System.exit(-1);
        } catch (NotBoundException e) {
            LOG.error("jobtracker not bind", e);
            System.exit(-1);
        } catch (UnknownHostException e) {
            LOG.error("can't resolve host name", e);
            System.exit(-1);
        }
    }

    private void taskFinished(Task task){
        task.setTaskTrackerName(taskTrackerInfo.toString());
        task.deleteTaskFolder();
    }

    private void runReducerTask(MapperTask mapperTask, ReducerTask reducerTask){
        TaskTrackerReducerWorker reducerWorker = new TaskTrackerReducerWorker(reducerTask, this);
        TaskTrackerReducerWorker oldReducerWorker = reducerWorkers.putIfAbsent(reducerTask.getTaskId(), reducerWorker);
        if(oldReducerWorker != null){
            reducerWorker = oldReducerWorker;
        } else {
            reducerTask.setOutputDir(tempDir);
            reducerWorker.createFolders();
            increaseReducerTaskAmount();
        }
        reducerWorker.addMapperTask(mapperTask);
        reducerWorker.updateReducerTask(reducerTask);
        threadPool.execute(reducerWorker);
    }

    public static void main(String[] args) throws Exception {
        TaskTracker taskTracker = new TaskTracker();
        JCommander commander = new JCommander(taskTracker, args);
        commander.setProgramName("mapreduce-tasktracker");
        if(taskTracker.needHelp()){
            commander.usage();
        } else {
            taskTracker.start();
        }
    }
}