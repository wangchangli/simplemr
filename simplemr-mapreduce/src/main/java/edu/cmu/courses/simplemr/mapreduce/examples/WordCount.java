package edu.cmu.courses.simplemr.mapreduce.examples;

import edu.cmu.courses.simplemr.mapreduce.common.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.io.OutputCollector;

import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount implements MapReduce {
    @Override
    public void map(String key, String value, OutputCollector collector) {
        StringTokenizer tokenizer = new StringTokenizer(value);
        String word;
        while (tokenizer.hasMoreTokens()) {
            word = tokenizer.nextToken();
            collector.collect(word, "one");
        }
    }

    @Override
    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int sum = 0;
        while (values.hasNext()) {
            values.next();
            sum += 1;
        }
        collector.collect(key, Integer.toString(sum));
    }
}
