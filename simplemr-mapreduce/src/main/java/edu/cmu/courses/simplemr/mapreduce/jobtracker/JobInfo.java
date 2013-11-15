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

public class JobInfo {
    private static AtomicInteger maxId = new AtomicInteger(0);

    private int id;
    private JobConfig config;
    private JobStatus status;
    private Map<Integer, MapperTask> mapperTasks;
    private Map<Integer, ReducerTask> reducerTasks;

    public JobInfo(JobConfig jobConfig){
        this.id = maxId.getAndIncrement();
        this.config = jobConfig;
        this.status = JobStatus.PENDING;
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
}
