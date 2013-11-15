package edu.cmu.courses.simplemr.mapreduce;

import edu.cmu.courses.simplemr.mapreduce.JobConfig;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobClientService extends Remote {
    public Pair<String, Integer> getFileServerInfo() throws RemoteException;
    public void submitJob(JobConfig jobConfig) throws RemoteException;
    public String describeJobs() throws RemoteException;
}
