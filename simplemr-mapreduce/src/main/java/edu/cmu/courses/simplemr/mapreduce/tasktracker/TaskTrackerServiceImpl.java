package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

public class TaskTrackerServiceImpl extends UnicastRemoteObject implements TaskTrackerService {

    private TaskTracker taskTracker;


    public TaskTrackerServiceImpl(TaskTracker taskTracker)
            throws RemoteException {
        super();
        this.taskTracker = taskTracker;
    }

    @Override
    public void runMapperTask(MapperTask task)
            throws RemoteException {
        taskTracker.runMapperTask(task);
    }

    @Override
    public void runReducerTask(MapperTask mapperTask, List<ReducerTask> reducerTasks)
            throws RemoteException {
        taskTracker.runReducerTask(mapperTask, reducerTasks);
    }
}
