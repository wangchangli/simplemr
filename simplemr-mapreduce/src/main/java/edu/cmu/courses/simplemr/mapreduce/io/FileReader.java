package edu.cmu.courses.simplemr.mapreduce.io;

/**
 * Abstract super class of DFS File Reader.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public abstract class FileReader {
    protected FileBlock fileBlock;

    public FileReader(FileBlock fileBlock){
        this.fileBlock = fileBlock;
    }

    public abstract void open() throws Exception;
    public abstract void close();
    public abstract String readLine() throws Exception;
}
