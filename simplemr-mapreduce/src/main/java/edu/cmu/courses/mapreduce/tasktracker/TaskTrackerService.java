package edu.cmu.courses.mapreduce.tasktracker;

import edu.cmu.courses.mapreduce.task.MapperTask;
import edu.cmu.courses.mapreduce.task.ReducerTask;
import edu.cmu.courses.mapreduce.task.Task;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskTrackerService extends Remote{
    public boolean runTask(Task task) throws RemoteException;
    public void reducerPrepare(MapperTask mapperTask, ReducerTask reducerTask) throws RemoteException;
}
