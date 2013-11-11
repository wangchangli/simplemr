package edu.cmu.courses.simplemr.mapreduce.task;

public class MapperTask extends Task {

    private String inputFile;
    private long fileOffset;
    private long fileSize;
    private String outputDir;
    private ReducerTask reducerTask;

    public MapperTask(int taskId, int jobId) {
        super(taskId, jobId, TaskType.MAPPER);
    }

    public ReducerTask getReducerTask(){
        return reducerTask;
    }
}
