package edu.cmu.courses.simplemr.mapreduce.tasktracker;

public class TaskTrackerHeartbeat implements Runnable {
    private TaskTracker taskTracker;

    public TaskTrackerHeartbeat(TaskTracker taskTracker){
        this.taskTracker = taskTracker;
    }

    @Override
    public void run() {
        taskTracker.heartbeat();
    }
}
