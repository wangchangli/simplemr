package edu.cmu.courses.mapreduce.tasktracker;

import edu.cmu.courses.mapreduce.task.MapperTask;
import edu.cmu.courses.mapreduce.task.ReducerTask;

import java.util.List;

public class TaskTracker {
    private TaskTrackerInfo taskTrackerInfo;
    private List<MapperTask> mapperTasks;
    private List<ReducerTask> reducerTasks;
}
