package edu.cmu.courses.simplemr.mapreduce.task;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Task superclass. The job tracker deliver a task to the task
 * tracker to let task tracker do the task.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */
public abstract class Task implements Serializable, Comparable<Task>{

    private static AtomicInteger maxId = new AtomicInteger();
    public static final String TASK_FOLDER_PREFIX = "simplemr_task_";

    protected int taskId;
    protected int jobId;
    protected TaskType type;
    protected TaskStatus status;
    protected int attemptCount;
    protected String taskTrackerName;
    protected String mrClassName;
    protected String outputDir;

    public Task(int jobId, TaskType type){
        setTaskId(maxId.getAndIncrement());
        setJobId(jobId);
        setType(type);
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

    public int getAttemptCount(){
        return attemptCount;
    }

    public void increaseAttemptCount(){
        attemptCount++;
    }

    public void setAttemptCount(int count){
        attemptCount = count;
    }

    public String getTaskTrackerName() {
        return taskTrackerName;
    }

    public void setTaskTrackerName(String name){
        taskTrackerName = name;
    }

    public void setTaskTrackerName(TaskTrackerInfo taskTracker) {
        taskTracker.addTask(this);
        this.taskTrackerName = taskTracker.toString();
    }

    public String getMRClassName() {
        return mrClassName;
    }

    public void setMRClassName(String className) {
        this.mrClassName = className;
    }

    public String getTaskFolderName(){
        return Constants.TASKS_FILE_URI + Constants.FILE_SEPARATOR + TASK_FOLDER_PREFIX + taskId;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public void createTaskFolder(){
        File folder = new File(outputDir + Constants.FILE_SEPARATOR + getTaskFolderName());
        if(folder.exists()){
            folder.delete();
        }
        folder.mkdirs();
    }

    public void deleteTaskFolder(){
        File folder = new File(outputDir + Constants.FILE_SEPARATOR + getTaskFolderName());
        if(folder.exists()){
            folder.delete();
        }
    }
}