package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;

public class JobTrackerDispatcher implements Runnable {
    private JobTracker jobTracker;
    private MapperTask task;

    public JobTrackerDispatcher(JobTracker jobTracker, MapperTask task){
        this.jobTracker = jobTracker;
        this.task = task;
    }

    @Override
    public void run() {
        jobTracker.dispatchMapperTask(task);
    }
}
