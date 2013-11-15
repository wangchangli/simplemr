package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.mapreduce.JobClientService;
import edu.cmu.courses.simplemr.mapreduce.JobConfig;
import edu.cmu.courses.simplemr.mapreduce.Pair;

import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobClientServiceImpl extends UnicastRemoteObject implements JobClientService {

    private JobTracker jobTracker;

    public JobClientServiceImpl(JobTracker jobTracker) throws RemoteException {
        super();
        this.jobTracker = jobTracker;
    }


    @Override
    public Pair<String, Integer> getFileServerInfo() throws RemoteException {
        try {
            String host = Utils.getHost();
            return new Pair<String, Integer>(host, jobTracker.getFileServerPort());
        } catch (UnknownHostException e) {
            throw new RemoteException("can't get host name");
        }
    }

    @Override
    public boolean submitJob(JobConfig jobConfig) throws RemoteException {
        try {
            jobTracker.submitJob(jobConfig);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String describeJobs() throws RemoteException{
        return jobTracker.describeJobs();
    }
}
