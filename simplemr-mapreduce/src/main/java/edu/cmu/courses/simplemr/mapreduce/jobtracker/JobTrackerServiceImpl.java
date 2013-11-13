package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.mapreduce.JobTrackerService;
import edu.cmu.courses.simplemr.mapreduce.common.JobConfig;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobTrackerServiceImpl extends UnicastRemoteObject implements JobTrackerService {

    private JobTracker jobTracker;

    public JobTrackerServiceImpl(JobTracker jobTracker) throws RemoteException {
        super();
        this.jobTracker = jobTracker;
    }

    @Override
    public int newJobId() throws RemoteException {
        return JobInfo.generateId();
    }

    @Override
    public boolean submitJob(JobConfig jobConfig) throws RemoteException {
        return jobTracker.submitJob(jobConfig);
    }

    @Override
    public void heartbeat(TaskTrackerInfo taskTrackerInfo) throws RemoteException {
        jobTracker.updateTaskTracker(taskTrackerInfo);
    }

    @Override
    public void taskSucceed(int taskId) throws RemoteException {
        jobTracker.taskSucceed(taskId);
    }

    @Override
    public void taskFailure(int taskId) throws RemoteException {
        jobTracker.taskFailure(taskId);
    }

    @Override
    public void reducerPrepareFailure(int failedMapperTaskId) throws RemoteException {
        //TODO needs implementation.
    }
}
