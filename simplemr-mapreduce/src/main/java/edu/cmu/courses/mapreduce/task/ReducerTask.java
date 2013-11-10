package edu.cmu.courses.mapreduce.task;

import java.util.List;

public class ReducerTask extends Task {

    private String outputFilePrefix;
    private List<MapperTask> mapperTasks;

    public ReducerTask(int taskId, int jobId) {
        super(taskId, jobId, TaskType.REDUCER);
    }
}
