package edu.cmu.courses.simplemr.mapreduce.tasktracker;

/**
 * Heart beat mechanism for Task Tracker.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

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
