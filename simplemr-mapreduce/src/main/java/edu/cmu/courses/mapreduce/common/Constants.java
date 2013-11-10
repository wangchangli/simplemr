package edu.cmu.courses.mapreduce.common;

public class Constants {
    public static final long HEARTBEAT_PERIOD = 10000;
    public static final long HEARTBEAT_INVALID = 3 * HEARTBEAT_PERIOD;
    public static final long HEARTBEAT_CHECK = 2 * HEARTBEAT_INVALID;
    public static final int DEFAULT_SCHEDULED_THREAD_POOL_SIZE = 4;
    public static final int DEFAULT_THREAD_POOL_SIZE = 16;
    public static final int DEFAULT_REGISTRY_PORT = 15440;
    public static final String DEFAULT_REGISTRY_HOST = "localhost";
}
