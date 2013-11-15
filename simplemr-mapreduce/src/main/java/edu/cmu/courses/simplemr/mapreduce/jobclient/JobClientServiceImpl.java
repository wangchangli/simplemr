package edu.cmu.courses.simplemr.mapreduce.jobclient;

import edu.cmu.courses.simplemr.mapreduce.JobClientService;
import edu.cmu.courses.simplemr.mapreduce.JobConfig;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobClientServiceImpl extends UnicastRemoteObject implements JobClientService {

    private JobClient jobClient;

    public JobClientServiceImpl(JobClient jobClient) throws RemoteException {
        super();
        this.jobClient = jobClient;
    }

    @Override
    public String getFileServerInfo() throws RemoteException {
        return null;
    }

    @Override
    public void submitJob(JobConfig jobConfig) throws RemoteException {

    }
}
