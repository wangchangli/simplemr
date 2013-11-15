package edu.cmu.courses.simplemr.mapreduce.task;

import edu.cmu.courses.simplemr.mapreduce.io.FileBlock;

/**
 * The Mapper Task extends the Task superclass. It contains
 * the parameters need to perform a mapper task.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class MapperTask extends Task {

    public static final String PARTITION_FILE_PREFIX = "PART_";

    private FileBlock inputFileBlock;
    private int reducerAmount;
    private String fileServerHost;
    private int fileServerPort;

    public MapperTask(int jobId, FileBlock inputFileBlock, int reducerAmount) {
        super(jobId, TaskType.MAPPER);
        this.inputFileBlock = inputFileBlock;
        this.reducerAmount = reducerAmount;
    }

    public FileBlock getInputFileBlock(){
        return inputFileBlock;
    }

    public int getReducerAmount(){
        return reducerAmount;
    }

    public String getFileServerHost() {
        return fileServerHost;
    }

    public void setFileServerHost(String fileServerHost) {
        this.fileServerHost = fileServerHost;
    }

    public int getFileServerPort() {
        return fileServerPort;
    }

    public void setFileServerPort(int fileServerPort) {
        this.fileServerPort = fileServerPort;
    }
}
