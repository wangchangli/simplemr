package edu.cmu.courses.simplemr.mapreduce;

import java.util.*;

public class OutputCollector {
    private PriorityQueue<Pair<String, String>> collection;
    private Set<String> keys;

    public OutputCollector() {
        collection = new PriorityQueue<Pair<String, String>>(10, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                if(o1.getKey().compareTo(o2.getKey()) == 0)
                    return o1.getValue().compareTo(o2.getValue());
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        keys = new TreeSet<String>();
    }

    public void collect(String key, String value){
        collection.add(new Pair<String, String>(key, value));
        keys.add(key);
    }

    public Iterator<Pair<String, String>> getIterator(){
        return collection.iterator();
    }

    public int getKeyCount(){
        return keys.size();
    }

    public TreeMap<String, List<String>> getMap(){
        TreeMap<String, List<String>> map = new TreeMap<String, List<String>>();
        for(Pair<String, String> pair : collection){
            if(map.containsKey(pair.getKey())){
                map.get(pair.getKey()).add(pair.getValue());
            } else {
                List<String> values = new ArrayList<String>();
                values.add(pair.getValue());
                map.put(pair.getKey(), values);
            }
        }
        return map;
    }
}
