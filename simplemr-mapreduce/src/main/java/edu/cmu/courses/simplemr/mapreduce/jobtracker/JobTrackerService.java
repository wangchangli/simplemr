package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.mapreduce.JobConfig;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobTrackerService extends Remote{
    public boolean submitJob(JobConfig jobConfig) throws RemoteException;
    public void heartbeat(TaskTrackerInfo taskTrackerInfo) throws RemoteException;
    public void mapperTaskSucceed(MapperTask task) throws RemoteException;
    public void reducerTaskSucceed(ReducerTask task) throws RemoteException;
    public void mapperTaskFailed(MapperTask task) throws RemoteException;
    public void reducerTaskFailed(ReducerTask task) throws RemoteException;
    public void reducerTaskFailedOnMapperTask(ReducerTask reducerTask, MapperTask mapperTask) throws RemoteException;
}
