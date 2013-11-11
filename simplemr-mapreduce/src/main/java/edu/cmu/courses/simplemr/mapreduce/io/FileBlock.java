package edu.cmu.courses.simplemr.mapreduce.io;

public class FileBlock {
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
