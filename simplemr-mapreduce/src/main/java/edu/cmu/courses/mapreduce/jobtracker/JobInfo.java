package edu.cmu.courses.mapreduce.jobtracker;

import edu.cmu.courses.mapreduce.common.JobConfig;
import edu.cmu.courses.mapreduce.task.Task;
import edu.cmu.courses.mapreduce.task.TaskStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JobInfo {
    private static AtomicInteger maxId;

    private int jobId;
    private JobConfig config;
    private JobStatus status;
    private List<Task> tasks;

    public JobInfo(int jobId, JobConfig jobConfig){
        this.jobId = jobId;
        this.config = jobConfig;
        this.status = JobStatus.INITIALIZING;
        this.tasks = new ArrayList<Task>();
    }

    public void addTask(Task task){
        tasks.add(task);
    }

    public void taskFinished(Task task){
        checkJobStatus();
    }

    public JobConfig getConfig(){
        return config;
    }

    public static int generateId(){
        return maxId.getAndIncrement();
    }

    private void checkJobStatus(){
        for(Task task : tasks){
            if(task.getStatus() == TaskStatus.INITIALIZING ||
               task.getStatus() == TaskStatus.PENDING){
                status = JobStatus.PENDING;
                return;
            } else if(task.getStatus() == TaskStatus.FAILED){
                status = JobStatus.FAILED;
                return;
            }
        }
        status = JobStatus.SUCCEED;
    }
}
