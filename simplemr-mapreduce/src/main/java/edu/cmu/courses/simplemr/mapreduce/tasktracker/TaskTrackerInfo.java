package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Include all the information about Task Tracker.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class TaskTrackerInfo implements Serializable{
    private String host;
    private int registryPort;
    private int fileServerPort;
    private int mapperTaskNumber;
    private int reduceTaskNumber;
    private long timestamp;
    private long invalidPeriod;
    private Set<Task> tasks;

    public TaskTrackerInfo(String host, int registryPort, int fileServerPort){
        this(host, registryPort, fileServerPort, Constants.DEFAULT_HEARTBEAT_INVALID);
    }

    public TaskTrackerInfo(String host, int registryPort, int fileServerPort, long invalidPeriod){
        this.host = host;
        this.registryPort = registryPort;
        this.fileServerPort = fileServerPort;
        this.mapperTaskNumber = 0;
        this.reduceTaskNumber = 0;
        this.timestamp = 0;
        this.invalidPeriod = invalidPeriod;
        this.tasks = new TreeSet<Task>();
    }

    public Task[] getTasks(){
        Task[] taskArray = new Task[tasks.size()];
        tasks.toArray(taskArray);
        return taskArray;
    }

    public List<Task> getPendingTasks(){
        List<Task> pendingTasks = new ArrayList<Task>();
        for(Task task : tasks){
            if(task.getStatus() == TaskStatus.PENDING){
                pendingTasks.add(task);
            }
        }
        return pendingTasks;
    }

    public List<MapperTask> getPendingMapperTask(){
        List<MapperTask> pendingMapperTasks = new ArrayList<MapperTask>();
        for(Task task : tasks){
            if(task instanceof MapperTask && task.getStatus() == TaskStatus.PENDING){
                pendingMapperTasks.add((MapperTask)task);
            }
        }
        return pendingMapperTasks;
    }

    public List<ReducerTask> getPendingReducerTask(){
        List<ReducerTask> pendingReducerTasks = new ArrayList<ReducerTask>();
        for(Task task : tasks){
            if(task instanceof ReducerTask && task.getStatus() == TaskStatus.PENDING){
                pendingReducerTasks.add((ReducerTask)task);
            }
        }
        return pendingReducerTasks;
    }

    public void addTask(Task task){
        tasks.add(task);
    }


    public void removeTask(Task task){
        tasks.remove(task);
    }

    public String getHost() {
        return host;
    }

    public int getRegistryPort(){
        return registryPort;
    }

    public int getFileServerPort() {
        return fileServerPort;
    }

    public int getMapperTaskNumber() {
        return mapperTaskNumber;
    }

    public int getReducerTaskNumber() {
        return reduceTaskNumber;
    }

    public void increaseMapperTaskNumber(){
        mapperTaskNumber++;
    }

    public void increaseReducerTaskNumber(){
        reduceTaskNumber++;
    }

    public void decreaseMapperTaskNumber(){
        mapperTaskNumber--;
    }

    public void decreaseReducerTaskNumber(){
        reduceTaskNumber--;
    }

    public void setMapperTaskNumber(int number){
        mapperTaskNumber = number;
    }

    public void setReduceTaskNumber(int number){
        reduceTaskNumber = number;
    }

    public void setTimestamp(long timestamp){
        this.timestamp = timestamp;
    }

    public boolean isValid(){
        if(System.currentTimeMillis() - timestamp > invalidPeriod){
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object taskTrackerInfo){
        if(taskTrackerInfo instanceof TaskTrackerInfo){
            return (host.equals(((TaskTrackerInfo) taskTrackerInfo).getHost())) &&
                   (fileServerPort == ((TaskTrackerInfo) taskTrackerInfo).getFileServerPort());
        } else {
            return false;
        }
    }

    @Override
    public String toString(){
        return host + ":" + getFileServerPort();
    }

    @Override
    public int hashCode(){
        return toString().hashCode();
    }
}
