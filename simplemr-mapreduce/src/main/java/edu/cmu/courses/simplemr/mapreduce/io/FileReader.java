package edu.cmu.courses.simplemr.mapreduce.io;

public abstract class FileReader {
    protected FileBlock fileBlock;

    public FileReader(FileBlock fileBlock){
        this.fileBlock = fileBlock;
    }

    public abstract void open() throws Exception;
    public abstract void close();
    public abstract String readLine() throws Exception;
}
