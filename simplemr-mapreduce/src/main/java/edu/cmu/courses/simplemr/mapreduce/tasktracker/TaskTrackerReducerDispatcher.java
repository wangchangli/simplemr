package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TaskTrackerReducerDispatcher implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(TaskTrackerReducerDispatcher.class);

    private TaskTracker taskTracker;
    private Map<Integer, TaskTrackerReducerWorker> reducerWorkers;

    public TaskTrackerReducerDispatcher(TaskTracker taskTracker){
        this.taskTracker = taskTracker;
        this.reducerWorkers = new HashMap<Integer, TaskTrackerReducerWorker>();
    }

    @Override
    public void run() {
        while(true){
            try {
                Pair<MapperTask, ReducerTask> entry = taskTracker.takeReducerTask();
                TaskTrackerReducerWorker reducerWorker = null;
                synchronized (reducerWorkers){
                    reducerWorker = reducerWorkers.get(entry.getValue().getTaskId());
                    if(reducerWorker == null){
                        taskTracker.increaseReducerTaskAmount();
                        entry.getValue().setOutputDir(taskTracker.getTempDir());
                        reducerWorker = new TaskTrackerReducerWorker(entry.getValue(), taskTracker);
                        reducerWorkers.put(entry.getValue().getTaskId(), reducerWorker);
                    }

                    reducerWorker.addMapperTask(entry.getKey());
                    reducerWorker.updateReducerTask(entry.getValue());
                }
                taskTracker.getThreadPool().execute(reducerWorker);
            } catch (InterruptedException e) {
                LOG.error("task tracker's reducer dispatcher is interrupted!", e);
                System.exit(-1);
            }
        }

    }
}
