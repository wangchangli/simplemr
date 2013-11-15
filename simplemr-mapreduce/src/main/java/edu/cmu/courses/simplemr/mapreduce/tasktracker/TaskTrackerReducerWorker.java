package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.dfs.DFSClient;
import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.io.DFSFileWriter;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TaskTrackerReducerWorker extends TaskTrackerWorker {

    public static final String MAPPER_RESULTS_DIR = "mappers";

    private PriorityQueue<MapperTask> mapperTasks;
    private ConcurrentHashMap<Integer, String> mapperFiles;
    private ConcurrentHashMap<Integer, Integer> mapperLocks;
    private Boolean finished;

    public TaskTrackerReducerWorker(Task task, TaskTracker taskTracker) {
        super(task, taskTracker);
        this.mapperTasks = new PriorityQueue<MapperTask>();
        this.mapperFiles = new ConcurrentHashMap<Integer, String>();
        this.mapperLocks = new ConcurrentHashMap<Integer, Integer>();
        this.finished = false;
        task.createTaskFolder();
        String mapperResultsPath = task.getTaskFolderName() + Constants.FILE_SEPARATOR + MAPPER_RESULTS_DIR;
        File mapperResultsFolder = new File(getAbsolutePath(mapperResultsPath));
        if(!mapperResultsFolder.exists()){
            mapperResultsFolder.mkdirs();
        }
    }

    public void addMapperTask(MapperTask mapperTask){
        synchronized (mapperTasks){
            mapperTasks.add(mapperTask);
        }
    }

    public void updateReducerTask(ReducerTask task){
        this.task.setAttemptCount(task.getAttemptCount());
    }

    @Override
    public void run() {
        if(mapperFiles.size() == ((ReducerTask)task).getMapperAmount()){
            return;
        }
        MapperTask mapperTask = getNextMapperTask();
        if(mapperTask == null){
            return;
        }
        try {
            copyMapperResult(mapperTask);
        } catch (IOException e) {
            taskTracker.reducerFailedOnMapper(((ReducerTask)task), mapperTask);
            return;
        }

        synchronized (finished){
            try{
                if(mapperFiles.size() == ((ReducerTask)task).getMapperAmount() && (!finished)){
                    List<String> files = new ArrayList<String>(mapperFiles.values());
                    String unreducedFile = getAbsolutePath(getReducerResultFilePath("unreduced"));
                    String reducedFile = getAbsolutePath(getReducerResultFilePath(null));

                    Utils.mergeSortedFiles(files, unreducedFile);

                    OutputCollector collector = new OutputCollector();
                    MapReduce mr = newMRInstance();
                    reduce(unreducedFile, mr, collector);

                    saveResultToLocal(reducedFile, collector);
                    saveResultToDFS(reducedFile);

                    taskTracker.reducerSucceed(((ReducerTask)task));
                    finished = true;
                }
            } catch (Exception e){
                taskTracker.reducerFailed(((ReducerTask)task));
            }
        }
    }

    private MapperTask getNextMapperTask(){
        MapperTask mapperTask = null;
        synchronized (mapperTasks){
            mapperTask = mapperTasks.poll();
        }
        return mapperTask;
    }

    private String getAbsolutePath(String fileName){
        return task.getOutputDir() + Constants.FILE_SEPARATOR + fileName;
    }

    private Integer getMapperLock(MapperTask mapperTask){
        Integer lock = mapperLocks.putIfAbsent(mapperTask.getTaskId(), mapperTask.getTaskId());
        return lock == null ? mapperTask.getTaskId() : lock;
    }

    private String getMapperResultFilePath(MapperTask mapperTask){
        return task.getTaskFolderName() + Constants.FILE_SEPARATOR +
               MAPPER_RESULTS_DIR + Constants.FILE_SEPARATOR + "mapper_" + mapperTask.getTaskId() + "_" +
               MapperTask.PARTITION_FILE_PREFIX +
               ((ReducerTask)task).getPartitionIndex();
    }

    private String getMapperResultURI(MapperTask mapperTask){
        return mapperTask.getTaskFolderName().replaceAll(Constants.FILE_SEPARATOR, "/") + "/" +
               MapperTask.PARTITION_FILE_PREFIX + ((ReducerTask)task).getPartitionIndex();
    }

    private String getReducerResultFilePath(String suffix){
        return task.getTaskFolderName() + Constants.FILE_SEPARATOR + ((ReducerTask)task).getOutputFile() +
               "_" + ((ReducerTask)task).getPartitionIndex() + (suffix == null ? "" : "_" + suffix);
    }

    private String getReducerResultFileName(){
        return ((ReducerTask)task).getOutputFile() + "_" + ((ReducerTask)task).getPartitionIndex();
    }

    private void copyMapperResult(MapperTask mapperTask)
            throws IOException {
        synchronized (getMapperLock(mapperTask)){
            if(mapperFiles.contains(mapperTask.getTaskId())){
                return;
            }
            InputStream in = Utils.getRemoteFile(mapperTask.getFileServerHost(),
                                                 mapperTask.getFileServerPort(),
                                                 getMapperResultURI(mapperTask));
            String outputFile = getAbsolutePath(getMapperResultFilePath(mapperTask));
            FileOutputStream out = new FileOutputStream(outputFile);
            IOUtils.copy(in, out);
            in.close();
            out.close();
            mapperFiles.put(mapperTask.getTaskId(), outputFile);
        }
    }

    private void reduce(String inputFile, MapReduce mr, OutputCollector collector)
            throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line = null;
        String key = null;
        String prevKey = null;
        List<String> values = new ArrayList<String>();
        while(true){
            line = reader.readLine();
            if(line == null){
                if(key != null){
                    mr.reduce(key, values.iterator(), collector);
                }
                break;
            }
            Pair<String, String> entry = Utils.splitLine(line);
            key = entry.getKey();
            if(prevKey != null && !key.equals(prevKey)){
                mr.reduce(prevKey, values.iterator(), collector);
                values.clear();
            }
            values.add(entry.getValue());
            prevKey = key;
        }
    }

    private void saveResultToLocal(String localFileName, OutputCollector collector)
            throws IOException {
        FileWriter writer = new FileWriter(localFileName);
        Iterator<Pair<String, String>> iterator = collector.getIterator();
        while(iterator.hasNext()){
            Pair<String, String> entry = iterator.next();
            writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + entry.getValue() + "\n");
        }
        writer.flush();
        writer.close();
    }

    private void saveResultToDFS(String localFile)
            throws Exception {
        DFSClient dfsClient = new DFSClient(taskTracker.getRegistryHost(), taskTracker.getRegistryPort());
        dfsClient.connect();
        dfsClient.writeText(localFile, ((ReducerTask)task).getReplicas(), ((ReducerTask)task).getLineCount());
    }
}
