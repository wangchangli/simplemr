package edu.cmu.courses.simplemr.mapreduce;

import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.dfs.DFSConstants;

import java.io.Serializable;

/**
 * The configuration class of MapReduce.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobConfig implements Serializable {

    private static final int DEFAULT_ATTEMPT_COUNT = 3;

    private String jobName;
    private String className;
    private String inputFile;
    private String outputFile;
    private int outputFileReplica = DFSConstants.DEFAULT_REPLICA_NUMBER;
    private int outputFileBlockSize = DFSConstants.DEFAULT_LINE_COUNT;
    private int mapperAmount = 0;
    private int reducerAmount = 0;
    private int maxAttemptCount = DEFAULT_ATTEMPT_COUNT;

    public void validate(){
        Utils.validateString(jobName, "The name of job");
        Utils.validateString(className, "The name of class implemented MapReduce interface");
        Utils.validateString(inputFile, "The name of input file");
        Utils.validateString(outputFile, "The name of output file");
        Utils.validatePositiveInteger(outputFileReplica, "The replica amount of output file");
        Utils.validatePositiveInteger(outputFileBlockSize, "The block size of output file (count by line)");
        Utils.validatePositiveInteger(mapperAmount, "The amount of mappers");
        Utils.validatePositiveInteger(reducerAmount, "The amount of reducers");
        Utils.validatePositiveInteger(maxAttemptCount, "The max attempt count for failed job");
    }

    public int getMaxAttemptCount(){
        return maxAttemptCount;
    }

    public void setMaxAttemptCount(int maxAttemptCount){
        this.maxAttemptCount = maxAttemptCount;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public int getMapperAmount() {
        return mapperAmount;
    }

    public void setMapperAmount(int mapperAmount) {
        this.mapperAmount = mapperAmount;
    }

    public int getReducerAmount() {
        return reducerAmount;
    }

    public void setReducerAmount(int reducerAmount) {
        this.reducerAmount = reducerAmount;
    }

    public int getOutputFileReplica() {
        return outputFileReplica;
    }

    public void setOutputFileReplica(int outputFileReplica) {
        this.outputFileReplica = outputFileReplica;
    }

    public int getOutputFileBlockSize() {
        return outputFileBlockSize;
    }

    public void setOutputFileBlockSize(int outputFileBlockSize) {
        this.outputFileBlockSize = outputFileBlockSize;
    }
}
