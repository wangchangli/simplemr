package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.mapreduce.task.Task;

public class JobTrackerDispatcher implements Runnable {
    private JobTracker jobTracker;
    private Task task;

    public JobTrackerDispatcher(JobTracker jobTracker, Task task){
        this.jobTracker = jobTracker;
        this.task = task;
    }

    @Override
    public void run() {
        jobTracker.dispatchTask(task);
    }
}
