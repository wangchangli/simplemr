package edu.cmu.courses.mapreduce.tasktracker;

import edu.cmu.courses.mapreduce.task.MapperTask;
import edu.cmu.courses.mapreduce.task.ReducerTask;
import edu.cmu.courses.mapreduce.task.Task;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class TaskTrackerServiceImpl extends UnicastRemoteObject implements TaskTrackerService {

    private TaskTracker taskTracker;


    public TaskTrackerServiceImpl(TaskTracker taskTracker) throws RemoteException {
        super();
        this.taskTracker = taskTracker;
    }

    @Override
    public boolean runTask(Task taskInfo) throws RemoteException {
        return false;  //TODO needs implementation.
    }

    @Override
    public void reducerPrepare(MapperTask mapperTask, ReducerTask reducerTask) throws RemoteException {
        //TODO needs implementation.
    }
}
