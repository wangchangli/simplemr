package edu.cmu.courses.mapreduce.jobtracker;

import edu.cmu.courses.mapreduce.common.JobConfig;
import edu.cmu.courses.mapreduce.tasktracker.TaskTrackerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobTrackerService extends Remote{
    public int newJobId() throws RemoteException;
    public boolean submitJob(JobConfig jobConfig) throws RemoteException;

    public void heartbeat(TaskTrackerInfo taskTrackerInfo) throws RemoteException;
    public void taskSucceed(int taskId) throws RemoteException;
    public void taskFailure(int taskId) throws RemoteException;
    public void reducerPrepareFailure(int failedMapperTaskId) throws RemoteException;

}
