package edu.cmu.courses.simplemr.mapreduce.jobtracker;

/**
 * The job tracker worker take charge in generating map and reduce
 * tasks. And start task trackers by assigning map tasks.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobTrackerWorker implements Runnable {
    private JobTracker jobTracker;
    private int jobId;

    public JobTrackerWorker(JobTracker jobTracker, int jobId){
        this.jobTracker = jobTracker;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        try {
            jobTracker.startJob(jobId);
        } catch (Exception e) {
            jobTracker.startJobFailed(jobId);
        }
    }
}
