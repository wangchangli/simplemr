package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.common.MapReduceConstants;
import edu.cmu.courses.simplemr.mapreduce.fileserver.FileServer;
import edu.cmu.courses.simplemr.mapreduce.jobtracker.JobTrackerService;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;

public class TaskTracker {

    @Parameter(names = {"-rh", "--registry-host"}, description = "the registry host")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "the registry port")
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
    private PriorityBlockingQueue<Pair<MapperTask, ReducerTask>> reducersQueue;

    private TaskTrackerService taskTrackerService;
    private JobTrackerService jobTrackerService;
    private ExecutorService threadPool;
    private ScheduledExecutorService heartbeatPool;
    private Registry registry;

    public TaskTracker(){
        reducersQueue = new PriorityBlockingQueue<Pair<MapperTask, ReducerTask>>();
    }

    public void start()
            throws Exception {
        taskTrackerInfo = new TaskTrackerInfo(Utils.getHost(), fileServerPort, invalidPeriod);
        threadPool = Executors.newFixedThreadPool(threadPoolSize);
        heartbeatPool = Executors.newScheduledThreadPool(Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
        bindService();
        heartbeatPool.scheduleAtFixedRate(new TaskTrackerHeartbeat(this), 0, heartbeatPeriod, TimeUnit.MILLISECONDS);
        new FileServer(fileServerPort, tempDir).start();
        Thread reducerDispatcher = new Thread(new TaskTrackerReducerDispatcher(this));
        reducerDispatcher.start();
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
            reducersQueue.offer(new Pair<MapperTask, ReducerTask>(mapperTask, reducerTask));
        }
    }

    public void increaseReducerTaskAmount(){
        taskTrackerInfo.increaseReducerTaskNumber();
    }

    public Pair<MapperTask, ReducerTask> takeReducerTask()
            throws InterruptedException {
        return reducersQueue.take();
    }

    public ExecutorService getThreadPool(){
        return threadPool;
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

    public String getRegistryHost() {
        return registryHost;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public String getTempDir(){
        return tempDir;
    }

    public boolean needHelp(){
        return help;
    }

    private void bindService() {
        try{
            taskTrackerService = new TaskTrackerServiceImpl(this);
            registry = LocateRegistry.getRegistry(registryHost, registryPort);
            registry.rebind(taskTrackerInfo.toString(), taskTrackerService);
            jobTrackerService = (JobTrackerService)registry.lookup(JobTrackerService.class.getCanonicalName());
        } catch (RemoteException e){
            LOG.error("registry server error", e);
            System.exit(-1);
        } catch (NotBoundException e) {
            LOG.error("jobtracker not bind", e);
            System.exit(-1);
        }
    }

    private void taskFinished(Task task){
        task.setTaskTrackerName(taskTrackerInfo.toString());
        task.deleteTaskFolder();
    }

    public static void main(String[] args) throws Exception {
        TaskTracker taskTracker = new TaskTracker();
        JCommander commander = new JCommander(taskTracker, args);
        if(taskTracker.needHelp()){
            commander.usage();
        } else {
            taskTracker.start();
        }
    }
}