package edu.cmu.courses.simplemr.mapreduce.jobclient;

import edu.cmu.courses.simplemr.mapreduce.common.JobConfig;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobClientService extends Remote {
    public void submitJob(JobConfig jobConfig) throws RemoteException;
}
