package edu.cmu.courses.simplemr.dfs;

public class DFSConstants {
    public static final int DEFAULT_REPLICA_NUMBER = 3;
    public static final int DEFAULT_DATA_PORT = 15441;
    public static final String DEFAULT_MASTER_EDIT_LOG_PATH =
            System.getProperty("user.dir") +
            System.getProperty("file.separator") +
            "dfs_master_edit.log";
}
