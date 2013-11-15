package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.JobClientService;
import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.common.RemoteClassLoader;
import edu.cmu.courses.simplemr.mapreduce.task.Task;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

public abstract class TaskTrackerWorker implements Runnable {
    protected Task task;
    protected TaskTracker taskTracker;

    protected TaskTrackerWorker(Task task, TaskTracker taskTracker){
        this.task = task;
        this.taskTracker = taskTracker;
    }


    protected MapReduce newMRInstance()
            throws IllegalAccessException, InstantiationException, RemoteException, ClassNotFoundException {
        Class<?> mrClass = null;
        try {
            mrClass = Class.forName(task.getMRClassName());
        } catch (ClassNotFoundException e) {
            if((mrClass = loadRemoteClass()) == null){
                e.printStackTrace();
                throw e;
            }
        }
        return (MapReduce) mrClass.newInstance();
    }

    protected Class<?> loadRemoteClass(){
        try{
            Registry registry = taskTracker.getRegistry();
            JobClientService service = (JobClientService)registry.lookup(JobClientService.class.getCanonicalName());
            Pair<String, Integer> fileServerInfo = service.getFileServerInfo();
            RemoteClassLoader classLoader = new RemoteClassLoader();
            return classLoader.loadRemoteClass(fileServerInfo.getKey(), fileServerInfo.getValue(),
                    task.getMRClassName());
        } catch (IOException e){
            return null;
        } catch (NotBoundException e) {
            return null;
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
