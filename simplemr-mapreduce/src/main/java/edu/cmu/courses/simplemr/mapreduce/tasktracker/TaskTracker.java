package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.mapreduce.TaskTrackerService;
import edu.cmu.courses.simplemr.mapreduce.common.MapReduceConstants;
import edu.cmu.courses.simplemr.mapreduce.io.FileBlock;
import edu.cmu.courses.simplemr.mapreduce.io.LocalFileReader;
import edu.cmu.courses.simplemr.mapreduce.io.LocalFileWriter;
import edu.cmu.courses.simplemr.mapreduce.common.Pair;
import edu.cmu.courses.simplemr.mapreduce.io.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskType;

import java.lang.reflect.Method;
import java.net.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

public class TaskTracker {
    private TaskTrackerInfo taskTrackerInfo;
    private List<MapperTask> mapperTasks;
    private List<ReducerTask> reducerTasks;

    private TaskTrackerService service;
    private String registryHost;
    private int registryPort;
    private Registry registry;

    public TaskTracker(String rHost, int rPort, int localPort,
                       String mapperOutputDir, long invalidPeriod ) throws RemoteException, UnknownHostException {
        taskTrackerInfo = new TaskTrackerInfo(getLocalHost(), localPort,
                mapperOutputDir, invalidPeriod);
        service = new TaskTrackerServiceImpl(this);
        registryHost = rHost;
        registryPort = rPort;
        //bindService();
    }
    private void bindService()
            throws RemoteException {
        registry = LocateRegistry.getRegistry(registryHost, registryPort);
        registry.rebind(taskTrackerInfo.getHost() + ":"
                + Integer.toString(taskTrackerInfo.getPort()), service);
    }

    private static String getLocalHost()
            throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        return inetAddress.getHostName();
    }

    public boolean runTask(Task task) {
        try {
            if (task.getType() == TaskType.MAPPER) {
                doMapperWork(task);
            }
            if (task.getType() == TaskType.REDUCER) {
                doReducerWork(task);
            }
            reportSuccess();
        } catch (Exception e) {
            reportFailure();
        }
        return true;
    }

    private void reportFailure() {
        System.out.println("fail");
    }

    private void reportSuccess() {
        System.out.println("success");
    }


    private void doMapperWork(Task task) throws Exception {

        Class<?> mapClass = getClass(task, MapReduceConstants.CLASSNAME);

        Method mapMethod = getMethod(task, mapClass, MapReduceConstants.mapMethodName);
        OutputCollector collector = collectFromMapper(task, mapClass, mapMethod);
        saveToLocalFiles(task, collector);
    }



    private void saveToLocalFiles(Task task, OutputCollector collector) throws Exception {
        LocalFileWriter[] localFileWriter = new LocalFileWriter[task.getReducerNum()];
        for (int i = 0; i < task.getReducerNum(); i++) {
            localFileWriter[i] = new LocalFileWriter(
                    taskTrackerInfo.getMapperOutputDir() + task.getJobId() + "_" +
                            task.getTaskId() + "_" + Integer.toString(i) + ".txt");
        }
        Iterator<Pair<String, String>> it = collector.getIterator();
        Pair<String, String> entry;
        while (it.hasNext()) {
            entry = it.next();
            int hash = Math.abs(entry.getKey().hashCode()) % task.getReducerNum();
            localFileWriter[hash].writeLine(entry.getKey() + MapReduceConstants.delimiter + entry.getValue());
        }
        for (int i = 0; i < task.getReducerNum(); i++) {
            localFileWriter[i].close();
        }
    }


    private OutputCollector collectFromMapper(Task task, Class<?> mapClass, Method mapMethod)
            throws Exception {


         LocalFileReader fileReader = new LocalFileReader(new FileBlock(
                MapReduceConstants.inputFileName, MapReduceConstants.offset, MapReduceConstants.size));
        String line;

        String[] token;
        OutputCollector collector = new OutputCollector();
        Object mapObject = mapClass.newInstance();
        while ((line = fileReader.readLine()) != null) {
            token = line.split(MapReduceConstants.delimiter, 2);
            String key = token[0];
            String value = token[1];

            mapMethod.invoke(mapObject, key, value, collector);

        }
        return collector;
    }


    private Class<?> getClass(Task task, String className) throws MalformedURLException, ClassNotFoundException {
        return Class.forName(className);
    }


    private Method getMethod(Task task, Class<?> mapreduceClass, String methodName)
            throws MalformedURLException, ClassNotFoundException, NoSuchMethodException {
        Class params[] = new Class[3];
        params[0] = String.class;
        if(methodName.equals("map")){
            params[1] = String.class;
        }
        else{
            params[1] = Iterator.class;
        }
        params[2] = OutputCollector.class;
        return mapreduceClass.getDeclaredMethod(methodName, params);
    }

    private void doReducerWork(Task task) throws Exception {

        loadFileToLocal(task);

        if (((ReducerTask)task).getStartReduce()) {
            sortAndMerge(task);
            Class<?> reduceClass = getClass(task, MapReduceConstants.CLASSNAME);
            Method reduceMethod = getMethod(task, reduceClass, MapReduceConstants.reduceMethodName);

            OutputCollector collector = collectFromReducer(task, reduceClass, reduceMethod);
            saveToDFS(task, collector);
        }
    }

    private OutputCollector collectFromReducer(Task task, Class<?> reduceClass, Method reduceMethod)
            throws Exception {
        String reduceOutputDir = "simplemr-mapreduce/src/main/java/edu/cmu/courses/" +
                "simplemr/mapreduce/examples/ReducerOutput/";
        LocalFileReader fileReader = new LocalFileReader(new FileBlock(
                reduceOutputDir + task.getJobId() + "_" + ((ReducerTask)task).getPartition()+ ".txt", MapReduceConstants.offset, MapReduceConstants.size));
        String line;

        OutputCollector collector = new OutputCollector();

        String[] token;
        List<String> values = new ArrayList<String>();
        Iterator<String> it;
        String prevKey = null;
        String key = null;
        String value = null;
        Object reduceObject = reduceClass.newInstance();
        do {
            line = fileReader.readLine();
            if(line != null)
            {
                token = line.split(MapReduceConstants.delimiter, 2);
                key = token[0];
                value = token[1];
            }

            if (!key.equals(prevKey) || line == null) {
                if (prevKey != null) {
                    it = values.iterator();
                    reduceMethod.invoke(reduceObject, prevKey, it, collector);
                }
                if(line == null)
                    break;
                prevKey = key;
                values.clear();
            }
            values.add(value);

        } while (true);
        return collector;
    }

    private void saveToDFS(Task task, OutputCollector collector) throws Exception {
         //TO DO: change from local file system to DFS
        String reduceOutputDir = "simplemr-mapreduce/src/main/java/edu/cmu/courses/" +
                "simplemr/mapreduce/examples/ReducerOutput/";
        LocalFileWriter localFileWriter = new LocalFileWriter(
                reduceOutputDir + task.getJobId() + "_" + ((ReducerTask)task).getPartition() + "_result.txt");
        Iterator<Pair<String, String>> it = collector.getIterator();
        Pair<String, String> entry;
        while (it.hasNext()) {
            entry = it.next();
            localFileWriter.writeLine(entry.getKey() + MapReduceConstants.delimiter + entry.getValue());
        }
        localFileWriter.close();

    }


    private void sortAndMerge(Task task) throws Exception {
        ReducerTask reducerTask = (ReducerTask)task;
        int MapperNum = task.getMapperNum();
        LocalFileReader[] fileReader = new LocalFileReader[MapperNum];

        String line;
        List<String> lines = new ArrayList<String>();
        for (int i = 0; i < MapperNum; i++) {
            fileReader[i] = new LocalFileReader(new FileBlock(reducerTask.getReducerInputDir() +
                    reducerTask.getJobId() + "_" + Integer.toString(i) + "_" +
                    Integer.toString(reducerTask.getPartition()) + ".txt",
                    MapReduceConstants.offset, MapReduceConstants.size));
            while((line = fileReader[i].readLine()) != null) {
                lines.add(line);
            }

            fileReader[i].close();
        }
        Collections.sort(lines);
        String reduceOutputDir = "simplemr-mapreduce/src/main/java/edu/cmu/courses/" +
                "simplemr/mapreduce/examples/ReducerOutput/";
        String[] strArr= lines.toArray(new String[0]);
        LocalFileWriter fileWriter = new LocalFileWriter(
                reduceOutputDir + task.getJobId() + "_" + ((ReducerTask) task).getPartition() + ".txt");
        for (String cur : strArr) {
            fileWriter.writeLine(cur);
        }
        fileWriter.close();
    }

    private void loadFileToLocal(Task task) {
           //TO DO: load mapper output to reducer local
    }
}