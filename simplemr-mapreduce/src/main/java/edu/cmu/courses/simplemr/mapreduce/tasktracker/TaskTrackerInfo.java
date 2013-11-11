package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.common.Constants;
import edu.cmu.courses.simplemr.mapreduce.task.Task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TaskTrackerInfo implements Serializable{
    private String host;
    private int port;
    private int mapperTaskNumber;
    private int reduceTaskNumber;
    private String mapperOutputDir;
    private long timestamp;
    private long invalidPeriod;
    private List<Task> tasks;

    public TaskTrackerInfo(String host, int port, String mapperOutputDir){
        this(host, port, mapperOutputDir, Constants.HEARTBEAT_INVALID);
    }

    public TaskTrackerInfo(String host, int port, String mapperOutputDir, long invalidPeriod){
        this.host = host;
        this.port = port;
        this.mapperOutputDir = mapperOutputDir;
        this.mapperTaskNumber = 0;
        this.reduceTaskNumber = 0;
        this.timestamp = 0;
        this.invalidPeriod = invalidPeriod;
        this.tasks = new ArrayList<Task>();
    }

    public Task[] getTasks(){
        Task[] taskArray = new Task[tasks.size()];
        tasks.toArray(taskArray);
        return taskArray;
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

    public int getPort() {
        return port;
    }

    public int getMapperTaskNumber() {
        return mapperTaskNumber;
    }

    public int getReduceTaskNumber() {
        return reduceTaskNumber;
    }

    public void setMapperTaskNumber(int mapperTaskNumber) {
        this.mapperTaskNumber = mapperTaskNumber;
    }

    public void setReduceTaskNumber(int reduceTaskNumber) {
        this.reduceTaskNumber = reduceTaskNumber;
    }

    public String getMapperOutputDir() {
        return mapperOutputDir;
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
                   (port == ((TaskTrackerInfo) taskTrackerInfo).getPort());
        } else {
            return false;
        }
    }

    @Override
    public String toString(){
        return host + ":" + port;
    }

    @Override
    public int hashCode(){
        return toString().hashCode();
    }
}
