package edu.cmu.courses.simplemr.mapreduce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Job {
    private static Logger LOG = LoggerFactory.getLogger(Job.class);

    private String registryHost;
    private int registryPort;
    private JobConfig jobConfig;
    private JobClientService jobClient;

    public Job(String registryHost, int registryPort, JobConfig jobConfig){
        this.jobConfig = jobConfig;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public void run(){
        jobConfig.validate();
        try {
            Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
            jobClient = (JobClientService) registry.lookup(JobClientService.class.getCanonicalName());
            jobClient.submitJob(jobConfig);
        } catch (RemoteException e) {
            LOG.error("failed to run mapreduce job", e);
        } catch (NotBoundException e) {
            LOG.error("the JobClientService is not bound in registry", e);
        }
    }
}
