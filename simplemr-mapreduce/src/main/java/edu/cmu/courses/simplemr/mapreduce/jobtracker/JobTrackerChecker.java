package edu.cmu.courses.simplemr.mapreduce.jobtracker;

/**
 * The job tracker periodically check the status of
 * task trackers.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobTrackerChecker implements Runnable {
    private JobTracker jobTracker;

    public JobTrackerChecker(JobTracker jobTracker){
        this.jobTracker = jobTracker;
    }

    @Override
    public void run() {
        jobTracker.checkTaskTrackers();
    }
}
