package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.io.DFSFileReader;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TaskTrackerMapperWorker extends TaskTrackerWorker{
    private DFSFileReader reader;

    public TaskTrackerMapperWorker(Task task, TaskTracker taskTracker) {
        super(task, taskTracker);
        reader = new DFSFileReader(taskTracker.getDfsMasterRegistryHost(),
                                   taskTracker.getDfsMasterRegistryPort(),
                                   ((MapperTask)task).getInputFileBlock());
    }

    @Override
    public void run() {
        try {
            OutputCollector collector = collect();
            saveToLocal(collector);
            taskTracker.mapperSucceed((MapperTask) task);
        } catch (Exception e) {
            taskTracker.mapperFailed((MapperTask) task);
        }
    }

    private OutputCollector collect()
            throws Exception {
        String line = null;
        MapReduce mr = newMRInstance();
        OutputCollector collector = new OutputCollector();
        reader.open();
        while((line = reader.readLine()) != null){
            Pair<String, String> entry = Utils.splitLine(line);
            mr.map(entry.getKey(), line, collector);
        }
        reader.close();
        return collector;
    }

    private void saveToLocal(OutputCollector collector)
            throws IOException {
        String folderName = ((MapperTask)task).getOutputDir() + Constants.FILE_SEPARATOR + task.getTaskFolderName();
        File[] outputFiles = new File[((MapperTask)task).getReducerAmount()];
        for(int i = 0; i < outputFiles.length; i++){
            outputFiles[i] = new File(folderName + Constants.FILE_SEPARATOR + MapperTask.PARTITION_FILE_PREFIX + i);
            outputFiles[i].createNewFile();
        }

        TreeMap<String, List<String>> recordMap = collector.getMap();
        int mapSize = recordMap.size();
        int rangeCount = Math.min(recordMap.size(), ((MapperTask)task).getReducerAmount());
        int rangeSize = mapSize / rangeCount;
        for(int i = 0; i < rangeCount; i++){
            int startKeyIndex = i * rangeSize;
            int endKeyIndex = (i + 1) * rangeSize;
            if(i == rangeCount - 1){
                endKeyIndex = mapSize;
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFiles[i]));
            for(int j = startKeyIndex; j < endKeyIndex; j++){
                Map.Entry<String, List<String>> entry = recordMap.pollFirstEntry();
                List<String> values = entry.getValue();
                for(String value : values){
                    writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + value);
                    writer.newLine();
                }
            }
            writer.flush();
            writer.close();
        }
    }
}
