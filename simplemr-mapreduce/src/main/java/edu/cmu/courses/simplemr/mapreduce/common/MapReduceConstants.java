package edu.cmu.courses.simplemr.mapreduce.common;

public class MapReduceConstants {
    public static final String inputFileName = "simplemr-mapreduce/" +
            "src/main/java/edu/cmu/courses/simplemr/mapreduce/examples/MapperInput/testInput.txt";

    public static final String outputFileName = "testOutput.txt";
    public static final long offset = 0;
    public static final long size = 100;
    public static final String CLASSNAME = "edu.cmu.courses.simplemr.mapreduce.examples.WordCount";
    public static final String delimiter = " ";
    public static final String mapClassName = "Map";
    public static final String reduceClassName = "Reduce";
    public static final String mapMethodName = "map";
    public static final String reduceMethodName = "reduce";
    public static final String reducerInputFileName = "testReducerInput";
    public static final String reducerTempFileName = "testReducerTempFile";
    public static final String TASK_TRACKER_OUTPUT_DIR = "/media/Documents/Graduate/15-640/Project3";
    public static final String DEFINED_CLASS_NAME = "MyClass";
    public static final String REGISTRY_HOST = "registry";
    public static final int REGISTRY_PORT = 12345;
}
