package edu.cmu.courses.simplemr.mapreduce;

import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskTrackerService extends Remote{
    public boolean runTask(Task task) throws RemoteException;
    public void reducerPrepare(MapperTask mapperTask, ReducerTask reducerTask) throws RemoteException;
}
