package edu.cmu.courses.mapreduce.common;

public class Record implements Comparable<Record> {
    private String key;
    private String value;

    public Record(String key, String value){
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(Record o) {
        return this.key.compareTo(o.key);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
