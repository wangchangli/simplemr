package edu.cmu.courses.simplemr;

public class Constants {
    public static final long DEFAULT_HEARTBEAT_PERIOD = 60000;
    public static final long DEFAULT_HEARTBEAT_INVALID = 3 * DEFAULT_HEARTBEAT_PERIOD;
    public static final long HEARTBEAT_CHECK = 2 * DEFAULT_HEARTBEAT_INVALID;
    public static final int DEFAULT_SCHEDULED_THREAD_POOL_SIZE = 4;
    public static final int DEFAULT_THREAD_POOL_SIZE = 16;
    public static final int DEFAULT_DATA_PORT = 15441;
    public static final int DEFAULT_REGISTRY_PORT = 1099;
    public static final String DEFAULT_REGISTRY_HOST = "localhost";

}
