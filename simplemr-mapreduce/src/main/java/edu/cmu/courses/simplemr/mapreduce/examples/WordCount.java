package edu.cmu.courses.simplemr.mapreduce.examples;

import edu.cmu.courses.simplemr.mapreduce.JobConfig;
import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.jobtracker.JobTrackerService;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;

public class WordCount implements MapReduce {
    @Override
    public void map(String key, String value, OutputCollector collector) {
        String[] words = value.split("\\s+");
        for(String word : words){
            collector.collect(word, "1");
        }
    }

    @Override
    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int count = 0;
        while(values.hasNext()){
            count++;
            values.next();
        }
        collector.collect(key, String.valueOf(count));
    }

    public static void main(String[] args) throws RemoteException, NotBoundException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName("WordCount");
        jobConfig.setClassName(WordCount.class.getCanonicalName());
        jobConfig.setMapperAmount(20);
        jobConfig.setReducerAmount(10);
        jobConfig.setInputFile("test.txt");
        jobConfig.setOutputFile("test_result");
        Registry registry = LocateRegistry.getRegistry("localhost", 1099);
        JobTrackerService service = (JobTrackerService)registry.lookup(JobTrackerService.class.getCanonicalName());
        service.submitJob(jobConfig);
    }
}
