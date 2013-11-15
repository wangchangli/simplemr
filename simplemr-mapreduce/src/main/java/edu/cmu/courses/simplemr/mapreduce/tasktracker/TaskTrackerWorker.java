package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.task.Task;

public abstract class TaskTrackerWorker implements Runnable {
    protected Task task;
    protected TaskTracker taskTracker;

    protected TaskTrackerWorker(Task task, TaskTracker taskTracker){
        this.task = task;
        this.taskTracker = taskTracker;
    }

    protected MapReduce newMRInstance()
            throws IllegalAccessException, InstantiationException {
        Class<?> mrClass = null;
        try {
            mrClass = Class.forName(task.getMRClassName());
        } catch (ClassNotFoundException e) {
            //TODO
        }
        return (MapReduce) mrClass.newInstance();
    }
}
