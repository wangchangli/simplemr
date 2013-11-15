package edu.cmu.courses.simplemr.mapreduce;

import edu.cmu.courses.simplemr.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Job {
    private static Logger LOG = LoggerFactory.getLogger(Job.class);

    private String registryHost;
    private int registryPort;
    private JobClientService jobClient;

    public Job(String registryHost, int registryPort){
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public void run(JobConfig jobConfig, Class<?> mapReduceClass){
        jobConfig.validate();
        try {
            Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
            jobClient = (JobClientService) registry.lookup(JobClientService.class.getCanonicalName());
            Pair<String, Integer> fileServerInfo = jobClient.getFileServerInfo();
            Utils.postClassFile(fileServerInfo.getKey(), fileServerInfo.getValue(), mapReduceClass);
            jobClient.submitJob(jobConfig);
        } catch (RemoteException e) {
            LOG.error("failed to run mapreduce job", e);
        } catch (NotBoundException e) {
            LOG.error("the JobClientService is not bound in registry", e);
        } catch (IOException e) {
            LOG.error("failed to send class file to job tracker");
        }
    }
}
