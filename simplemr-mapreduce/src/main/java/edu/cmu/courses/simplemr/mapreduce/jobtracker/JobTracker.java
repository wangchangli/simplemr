package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.JobTrackerService;
import edu.cmu.courses.simplemr.mapreduce.common.JobConfig;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;
import edu.cmu.courses.simplemr.mapreduce.TaskTrackerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.concurrent.*;

public class JobTracker {
    private Logger LOG = LoggerFactory.getLogger(JobTracker.class);

    private ConcurrentHashMap<Integer, TaskTrackerInfo> taskTackers;
    private ConcurrentHashMap<Integer, JobInfo> jobs;
    private ConcurrentHashMap<Integer, Task> tasks;
    private PriorityBlockingQueue<Task> taskDispatchQueue;
    private ScheduledExecutorService periodicalChecker;
    private JobTrackerService service;
    private Thread scheduler;
    private String registryHost;
    private int registryPort;
    private Registry registry;

    public JobTracker(String rHost, int rPort, int dispatcherThreadPoolSize)
            throws RemoteException {
        taskTackers = new ConcurrentHashMap<Integer, TaskTrackerInfo>();
        jobs = new ConcurrentHashMap<Integer, JobInfo>();
        tasks = new ConcurrentHashMap<Integer, Task>();
        taskDispatchQueue = new PriorityBlockingQueue<Task>();
        service = new JobTrackerServiceImpl(this);
        scheduler = new Thread(new JobTrackerScheduler(this, dispatcherThreadPoolSize));
        registryHost = rHost;
        registryPort = rPort;
    }

    public boolean submitJob(JobConfig jobConfig){
        return false;
    }

    public void updateTaskTracker(TaskTrackerInfo taskTrackerInfo){
        TaskTrackerInfo old = taskTackers.get(taskTrackerInfo.hashCode());
        old.setMapperTaskNumber(taskTrackerInfo.getMapperTaskNumber());
        old.setReduceTaskNumber(taskTrackerInfo.getReduceTaskNumber());
        old.setTimestamp(System.currentTimeMillis());
    }

    public void dispatchTask(Task task){
        if(!task.getTaskTrackerInfo().isValid()){
            migrateTask(task);
        } else {
            try {
                TaskTrackerInfo taskTrackerInfo = task.getTaskTrackerInfo();
                TaskTrackerService ttService = getTaskTrackerService(taskTrackerInfo);
                task.increaseAttemptCount();
                ttService.runTask(task);
            } catch (Throwable e) {
                proceedTaskFailure(task);
            }
        }
    }

    public void taskSucceed(int taskId){
        Task task = tasks.get(taskId);
        processTaskSucceed(task);
    }

    public void taskFailure(int taskId){
        Task task = tasks.get(taskId);
        proceedTaskFailure(task);
    }

    public Task takeTask()
            throws InterruptedException {
        return taskDispatchQueue.take();
    }

    public void start()
            throws RemoteException {
        bindService();
        startScheduler();
    }

    private void bindService()
            throws RemoteException {
        if(registryHost == null){
           registryHost = Constants.DEFAULT_REGISTRY_HOST;
        }
        if(registryPort <= 0){
            registryPort = Constants.DEFAULT_REGISTRY_PORT;
        }
        registry = LocateRegistry.getRegistry(registryHost, registryPort);
        registry.rebind(JobTrackerService.class.getCanonicalName(), service);
    }

    private void startScheduler(){
        scheduler.start();
        try {
            scheduler.join();
        } catch (InterruptedException e) {
            LOG.error("join of scheduler is interrupted", e);
        } finally {
            LOG.error("scheduler is exited!");
            System.exit(-1);
        }
    }

    private TaskTrackerInfo getTaskTracker(Task task){
        if(task instanceof MapperTask){
            return getMapperTaskTracker(task);
        } else {
            return getReducerTaskTracker(task);
        }
    }

    private TaskTrackerInfo getMapperTaskTracker(Task task){
        TaskTrackerInfo old = task.getTaskTrackerInfo();
        TaskTrackerInfo minTaskTracker = null;
        Iterator<TaskTrackerInfo> iterator = taskTackers.values().iterator();
        while(iterator.hasNext()){
            TaskTrackerInfo taskTracker = iterator.next();
            if((!taskTracker.equals(old)) && taskTracker.isValid() &&
               (minTaskTracker == null || taskTracker.getMapperTaskNumber() < minTaskTracker.getMapperTaskNumber())){
                minTaskTracker = taskTracker;
            }
        }
        return minTaskTracker;
    }

    private TaskTrackerInfo getReducerTaskTracker(Task task){
        TaskTrackerInfo old = task.getTaskTrackerInfo();
        TaskTrackerInfo minTaskTracker = null;
        Iterator<TaskTrackerInfo> iterator = taskTackers.values().iterator();
        while(iterator.hasNext()){
            TaskTrackerInfo taskTracker = iterator.next();
            if((!taskTracker.equals(old)) && taskTracker.isValid() &&
               (minTaskTracker == null || taskTracker.getReduceTaskNumber() < minTaskTracker.getReduceTaskNumber())){
                minTaskTracker = taskTracker;
            }
        }
        return minTaskTracker;
    }

    private boolean migrateTask(Task task){
        TaskTrackerInfo old = task.getTaskTrackerInfo();
        TaskTrackerInfo taskTracker = getTaskTracker(task);
        if(taskTracker != null){
            old.removeTask(task);
            task.setTaskTrackerInfo(taskTracker);
            task.setStatus(TaskStatus.WAITING);
            taskDispatchQueue.offer(task);
            LOG.debug("migrate task " + task.getTaskId() +
                      " from " + old.toString() +
                      " to " + taskTracker.toString());
            return true;
        } else {
            JobInfo job = jobs.get(task.getJobId());
            task.setStatus(TaskStatus.FAILED);
            job.taskFinished(task);
            return false;
        }
    }

    private void migrateTaskTracker(TaskTrackerInfo taskTracker){
        Task[] taskArray = taskTracker.getTasks();
        for(Task task : taskArray){
            if(task.getStatus() == TaskStatus.PENDING){
                 migrateTask(task);
            }
        }
    }

    private void processTaskSucceed(Task task){
        JobInfo job = jobs.get(task.getJobId());
        if(task instanceof MapperTask){
            ReducerTask reducerTask = ((MapperTask) task).getReducerTask();
            TaskTrackerInfo taskTracker = reducerTask.getTaskTrackerInfo();
            try {
                TaskTrackerService ttService = getTaskTrackerService(taskTracker);
                ttService.reducerPrepare((MapperTask)task, reducerTask);
            } catch (Throwable e){
                if(migrateTask(reducerTask)){
                    processTaskSucceed(task);
                }
            }
        }
        task.setStatus(TaskStatus.SUCCEED);
        job.taskFinished(task);
    }

    private void proceedTaskFailure(Task task){
        JobInfo job = jobs.get(task.getJobId());
        if(task.getAttemptCount() >= job.getConfig().getMaxAttemptCount()){
            task.setStatus(TaskStatus.FAILED);
            job.taskFinished(task);
        } else {
            task.setStatus(TaskStatus.PENDING);
            taskDispatchQueue.offer(task);
        }
    }

    private TaskTrackerService getTaskTrackerService(TaskTrackerInfo taskTracker)
            throws RemoteException, NotBoundException {
        return (TaskTrackerService) registry.lookup(taskTracker.toString());
    }
}
