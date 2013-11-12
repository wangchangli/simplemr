package edu.cmu.courses.simplemr.io;

import java.io.IOException;

public abstract class FileReader {
    protected FileBlock fileBlock;

    public FileReader(FileBlock fileBlock){
        this.fileBlock = fileBlock;
    }

    public abstract void open() throws Exception;
    public abstract void close();
    public abstract String readLine() throws Exception;
}
