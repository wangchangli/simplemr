package edu.cmu.courses.simplemr.mapreduce;

import edu.cmu.courses.simplemr.mapreduce.JobConfig;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The service interface that a job client provide to user applications.
 * It extends the Remote interface to be called by RMI.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public interface JobClientService extends Remote {
    public Pair<String, Integer> getFileServerInfo() throws RemoteException;
    public void submitJob(JobConfig jobConfig) throws RemoteException;
    public String describeJobs() throws RemoteException;
}
