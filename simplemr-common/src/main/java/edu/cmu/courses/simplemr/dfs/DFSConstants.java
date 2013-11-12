package edu.cmu.courses.simplemr.dfs;

public class DFSConstants {
    public static final int DEFAULT_REPLICA_NUMBER = 3;
    public static final int DEFAULT_BLOCK_SIZE = 4096;
    public static final int DEFAULT_LINE_COUNT = 100000;
    public static final String DEFAULT_MASTER_EDIT_LOG_PATH =
            System.getProperty("user.dir") +
            System.getProperty("file.separator") +
            "dfs_master_edit.log";
    public static final String DEFAULT_SLAVE_DATA_PATH =
            System.getProperty("user.dir") +
            System.getProperty("file.separator") +
            "dfs_slave_data";
    public static final String CHUNK_PREFIX = "CHUNK_";
}
