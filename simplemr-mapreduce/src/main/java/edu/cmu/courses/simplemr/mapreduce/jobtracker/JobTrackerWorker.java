package edu.cmu.courses.simplemr.mapreduce.jobtracker;

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
