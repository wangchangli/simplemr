package edu.cmu.courses.simplemr.mapreduce.io;

import edu.cmu.courses.simplemr.mapreduce.common.Pair;

import java.util.*;

public class OutputCollector {
    private List<Pair<String, String>> collection;

    public OutputCollector() {
        collection = new ArrayList<Pair<String, String>>();
    }

    public void collect(String key, String value){
        collection.add(new Pair<String, String>(key, value));
    }

    public Iterator<Pair<String, String>> getIterator(){
        return collection.iterator();
    }
}
