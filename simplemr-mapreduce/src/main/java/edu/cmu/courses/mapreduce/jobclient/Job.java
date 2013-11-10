package edu.cmu.courses.mapreduce.jobclient;

import edu.cmu.courses.mapreduce.common.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public class Job {
    private static Logger LOG = LoggerFactory.getLogger(Job.class);

    private JobConfig jobConfig;
    private JobClientService jobClient;

    public Job(JobConfig jobConfig){
        this.jobConfig = jobConfig;
    }

    public void run(){
        try {
            jobClient.submitJob(jobConfig);
        } catch (RemoteException e) {
            LOG.error("Failed to run mapreduce job", e);
        }
    }
}
