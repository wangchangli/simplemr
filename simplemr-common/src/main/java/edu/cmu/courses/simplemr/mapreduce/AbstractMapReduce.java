package edu.cmu.courses.simplemr.mapreduce;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.dfs.DFSConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * A class for configuring MapReduce interface. Every client application
 * inherent from this abstract class.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public abstract class AbstractMapReduce implements MapReduce {
    @Parameter(description = "InputFile OutputFile", required = true)
    protected List<String> files = new ArrayList<String>();

    @Parameter(names = {"-n", "--name"}, description = "the name of job")
    protected String jobName = AbstractMapReduce.class.getSimpleName();

    @Parameter(names = {"-rh", "--registry-host"}, description = "the registry host")
    protected String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "the registry port")
    protected int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-m", "--mapper-number"}, description = "the number of mapper")
    protected int mapperAmount = 10;

    @Parameter(names = {"-r", "--reducer-number"}, description = "the number of reducer")
    protected int reducerAmount = 5;

    @Parameter(names = {"-t", "--max-attempt"}, description = "the maximum count of failed attempts")
    protected int maxAttemptCount = 3;

    @Parameter(names = {"-d", "--file-duplicates"}, description = "the number of replica of output file in dfs")
    protected int replicas = DFSConstants.DEFAULT_REPLICA_NUMBER;

    @Parameter(names = {"-l", "--line-count"}, description = "the count of lines per file chunk in dfs")
    protected int lineCount = DFSConstants.DEFAULT_LINE_COUNT;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help = false;

    public JobConfig getJobConfig(){
        JobConfig jobConfig = new JobConfig();
        if(files.size() > 0){
            jobConfig.setInputFile(files.get(0));
        }
        if(files.size() > 1){
            jobConfig.setOutputFile(files.get(1));
        }
        jobConfig.setJobName(jobName);
        jobConfig.setMapperAmount(mapperAmount);
        jobConfig.setReducerAmount(reducerAmount);
        jobConfig.setMaxAttemptCount(maxAttemptCount);
        jobConfig.setOutputFileReplica(replicas);
        jobConfig.setOutputFileBlockSize(lineCount);
        jobConfig.setClassName(this.getClass().getName());
        jobConfig.validate();
        return jobConfig;
    }

    public boolean needHelp(){
        return help;
    }

    public void run(String[] args){
        JCommander commander = new JCommander(this, args);
        commander.setProgramName("simplemr-examples");
        if(needHelp()){
            commander.usage();
        } else {
            JobConfig jobConfig = getJobConfig();
            Job job = new Job(registryHost, registryPort);
            job.run(jobConfig, this.getClass());
        }
    }
}
