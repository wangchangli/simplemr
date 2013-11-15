package edu.cmu.courses.simplemr.mapreduce.task;

/**
 * The Reducer Task extends the Task superclass. It contains
 * the parameters need to perform a reduce task.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */
public class ReducerTask extends Task {

    private String outputFile;
    private int partitionIndex;
    private int mapperAmount;
    private int replicas;
    private int lineCount;

    public ReducerTask(int jobId) {
        super(jobId, TaskType.REDUCER);
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    public int getMapperAmount() {
        return mapperAmount;
    }

    public void setMapperAmount(int mapperAmount) {
        this.mapperAmount = mapperAmount;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }
}
