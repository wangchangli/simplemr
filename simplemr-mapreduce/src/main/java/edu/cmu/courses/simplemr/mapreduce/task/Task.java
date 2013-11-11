package edu.cmu.courses.simplemr.mapreduce.task;

import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;

import java.io.Serializable;

public abstract class Task implements Serializable, Comparable<Task>{
    protected int taskId;
    protected int jobId;
    protected TaskType type;
    protected TaskStatus status;
    protected TaskTrackerInfo taskTrackerInfo;
    protected int attemptCount;

    public Task(int taskId, int jobId, TaskType type){
        setTaskId(taskId);
        setJobId(jobId);
        setType(type);
        setStatus(TaskStatus.INITIALIZING);
        setTaskTrackerInfo(null);
        attemptCount = 0;
    }

    @Override
    public int compareTo(Task task) {
        return taskId - task.taskId;
    }

    @Override
    public boolean equals(Object task){
        if(task instanceof Task){
            return taskId == ((Task) task).getTaskId();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode(){
        return taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public TaskType getType() {
        return type;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public TaskTrackerInfo getTaskTrackerInfo() {
        return taskTrackerInfo;
    }

    public void setTaskTrackerInfo(TaskTrackerInfo taskTrackerInfo) {
        this.taskTrackerInfo = taskTrackerInfo;
    }

    public int getAttemptCount(){
        return attemptCount;
    }

    public void increaseAttemptCount(){
        attemptCount++;
    }
}
