package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;

/**
 * The service interface that a Job tracker can provide to
 * task tracker. It extends the Remote interface to be called by RMI.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

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
