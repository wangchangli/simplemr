package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.mapreduce.JobConfig;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JobInfo implements Comparable<JobInfo>{
    private static AtomicInteger maxId = new AtomicInteger(0);

    private int id;
    private JobConfig config;
    private JobStatus status;
    private Map<Integer, MapperTask> mapperTasks;
    private Map<Integer, ReducerTask> reducerTasks;

    public JobInfo(JobConfig jobConfig){
        this.id = maxId.getAndIncrement();
        this.config = jobConfig;
        this.status = JobStatus.INITIALIZING;
        this.mapperTasks = new TreeMap<Integer, MapperTask>();
        this.reducerTasks = new TreeMap<Integer, ReducerTask>();
    }

    public List<MapperTask> getMapperTasks(){
        return new ArrayList<MapperTask>(mapperTasks.values());
    }

    public List<ReducerTask> getReducerTasks(){
        return new ArrayList<ReducerTask>(reducerTasks.values());
    }

    public void setTaskStatus(int taskId, TaskStatus status){
        Task task = getTask(taskId);
        if(task != null){
            task.setStatus(status);
        }
    }

    public Task getTask(int taskId){
        Task task = mapperTasks.get(taskId);
        if(task == null){
            task = reducerTasks.get(taskId);
        }
        return task;
    }

    public JobStatus getStatus(){
        return status;
    }

    public void setJobStatus(JobStatus status){
        this.status = status;
    }

    public void addMapperTask(MapperTask mapperTask){
        mapperTasks.put(mapperTask.getTaskId(), mapperTask);
    }

    public void addReducerTask(ReducerTask reducerTask){
        reducerTasks.put(reducerTask.getTaskId(), reducerTask);
    }

    public int getId(){
        return id;
    }

    public JobConfig getConfig(){
        return config;
    }

    public String describeJob(){
        StringBuffer sb = new StringBuffer();
        JobStatus status = checkJobStatus();
        sb.append("#" + id);
        sb.append("\t");
        sb.append(config.getJobName());
        sb.append("\t");
        sb.append(status.toString());
        sb.append("\t(");
        sb.append(describeMapperTasks());
        sb.append(" || ");
        sb.append(describeReducerTasks());
        sb.append(")");
        return sb.toString();
    }

    private String describeMapperTasks(){
        int pendingCount = 0;
        int successCount = 0;
        int failureCount = 0;
        for(MapperTask mapperTask : getMapperTasks()){
            if(mapperTask.getStatus() == TaskStatus.PENDING){
                pendingCount++;
            }
            if(mapperTask.getStatus() == TaskStatus.SUCCEED){
                successCount++;
            }
            if(mapperTask.getStatus() == TaskStatus.FAILED){
                failureCount++;
            }
        }
        return "mapper task: " + pendingCount + " pending, " + successCount +
               " succeeded, " + failureCount + " failed";
    }

    private String describeReducerTasks(){
        int pendingCount = 0;
        int successCount = 0;
        int failureCount = 0;
        for(ReducerTask reducerTask : getReducerTasks()){
            if(reducerTask.getStatus() == TaskStatus.PENDING){
                pendingCount++;
            }
            if(reducerTask.getStatus() == TaskStatus.SUCCEED){
                successCount++;
            }
            if(reducerTask.getStatus() == TaskStatus.FAILED){
                failureCount++;
            }
        }
        return "reducer task: " + pendingCount + " pending, " + successCount +
                " succeeded, " + failureCount + " failed";
    }

    private JobStatus checkJobStatus(){
        boolean failure = false;
        boolean pending = false;
        if(status == JobStatus.INITIALIZING){
            return status;
        }
        for(Task task : getMapperTasks()){
            if(task.getStatus() == TaskStatus.PENDING){
                pending = true;
            }
            if(task.getStatus() == TaskStatus.FAILED){
                failure = true;
                break;
            }
        }

        for(Task task : getReducerTasks()){
            if(task.getStatus() == TaskStatus.PENDING){
                pending = true;
            }
            if(task.getStatus() == TaskStatus.FAILED){
                failure = true;
                break;
            }
        }

        if(failure){
            setJobStatus(JobStatus.FAILED);
            return status;
        }
        if(pending){
            setJobStatus(JobStatus.PENDING);
            return status;
        }
        setJobStatus(JobStatus.SUCCEED);
        return status;
    }

    @Override
    public int compareTo(JobInfo o) {
        return id - o.getId();
    }
}
