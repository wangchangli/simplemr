package edu.cmu.courses.simplemr.mapreduce.examples;

import edu.cmu.courses.simplemr.mapreduce.common.MapReduceConstants;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskType;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTracker;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;

import java.net.UnknownHostException;
import java.rmi.RemoteException;

public class TestTaskTracker {
    public static void main(String[] arg) throws RemoteException, UnknownHostException {


        //generate task
        int mapperNum = 2;
        int reducerNum = 2;
        int localPort = 12345;

        String interminateDir =  "simplemr-mapreduce/" +
                "src/main/java/edu/cmu/courses/simplemr/mapreduce/examples/MapperOutput/";
        for(int i = 0; i < mapperNum; i++) {
            String mapperOutputDir = interminateDir;
            int invalidPeriod = 0;
            TaskTracker taskTracker = new TaskTracker(MapReduceConstants.REGISTRY_HOST,
                    MapReduceConstants.REGISTRY_PORT, 0,
                    mapperOutputDir, invalidPeriod);

            Task mapperTask = new MapperTask(i, 0);
            mapperTask.setMapperNum(mapperNum);
            mapperTask.setReducerNum(reducerNum);
            taskTracker.runTask(mapperTask);
        }



        for(int i = 0; i < reducerNum; i++) {
            ReducerTask reducerTask = new ReducerTask(i, 0);
            reducerTask.setMapperNum(mapperNum);
            reducerTask.setReducerNum(reducerNum);
            reducerTask.setMapperTaskId(0);
            reducerTask.setReducerInputDir(interminateDir);
            reducerTask.setStartReduce(true);
            reducerTask.setPartition(i);
            TaskTracker taskTracker = new TaskTracker(MapReduceConstants.REGISTRY_HOST,
                    MapReduceConstants.REGISTRY_PORT, 0, null, 0);
            taskTracker.runTask(reducerTask);
        }

    }

}
