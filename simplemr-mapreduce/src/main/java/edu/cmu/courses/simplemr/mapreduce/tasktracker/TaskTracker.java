package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;

import java.util.List;

public class TaskTracker {
    private TaskTrackerInfo taskTrackerInfo;
    private List<MapperTask> mapperTasks;
    private List<ReducerTask> reducerTasks;
}
