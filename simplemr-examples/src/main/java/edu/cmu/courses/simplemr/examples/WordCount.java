package edu.cmu.courses.simplemr.examples;

import edu.cmu.courses.simplemr.mapreduce.AbstractMapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;

import java.util.Iterator;

public class WordCount extends AbstractMapReduce {

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

    public static void main(String[] args) {
        new WordCount().run(args);
    }
}
