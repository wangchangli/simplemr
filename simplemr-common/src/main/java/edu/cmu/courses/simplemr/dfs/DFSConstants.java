package edu.cmu.courses.simplemr.dfs;


/**
 * Constants used in distributed file system.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSConstants {
    public static final int DEFAULT_REPLICA_NUMBER = 3;
    public static final int DEFAULT_BLOCK_SIZE = (1 << 20);
    public static final int DEFAULT_LINE_COUNT = 40000;
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
