package edu.cmu.courses.simplemr.mapreduce.io;

import java.io.Serializable;

/**
 * Define a block of file.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class FileBlock implements Serializable {
    private String file;
    private long offset;
    private long size;

    public FileBlock(String file, long offset, long size){
        this.file = file;
        this.offset = offset;
        this.size = size;
    }

    public String getFile() {
        return file;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }
}
