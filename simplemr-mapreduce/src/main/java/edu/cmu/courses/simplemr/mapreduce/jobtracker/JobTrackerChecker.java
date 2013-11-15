package edu.cmu.courses.simplemr.mapreduce.jobtracker;

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
