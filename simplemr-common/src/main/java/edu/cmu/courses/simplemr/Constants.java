package edu.cmu.courses.simplemr;

/**
 * Define some parameters for MapReduce.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class Constants {
    public static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final long DEFAULT_HEARTBEAT_PERIOD = 1000;
    public static final long DEFAULT_HEARTBEAT_INVALID = 3 * DEFAULT_HEARTBEAT_PERIOD;
    public static final long HEARTBEAT_CHECK = 2 * DEFAULT_HEARTBEAT_INVALID;
    public static final int DEFAULT_SCHEDULED_THREAD_POOL_SIZE = 4;
    public static final int DEFAULT_THREAD_POOL_SIZE = 24;
    public static final int DEFAULT_REGISTRY_PORT = 10999;
    public static final String DEFAULT_REGISTRY_HOST = "localhost";
    public static final String CLASS_CONTENT_TYPE = "application/x-java-class";
    public static final String CLASS_FILE_URI = "classes";
    public static final String TASKS_FILE_URI = "tasks";
    public static final String MAPREDUCE_DELIMITER_REGEX = "\\s+";
    public static final String MAPREDUCE_DELIMITER = "\t";

}
