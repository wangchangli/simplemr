package edu.cmu.courses.mapreduce.common;

import edu.cmu.courses.mapreduce.io.OutputCollector;

import java.io.Serializable;
import java.util.Iterator;

public interface MapReduce extends Serializable{
    public void map(String key, String value, OutputCollector collector);
    public void reduce(String key, Iterator<String> values, OutputCollector collector);
}
