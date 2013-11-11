package edu.cmu.courses.simplemr.mapreduce.io;

import java.io.IOException;

public abstract class FileWriter {
    protected String file;

    public FileWriter(String file){
        this.file = file;
    }

    public abstract void open() throws IOException;
    public abstract void close();
    public abstract void writeLine(String line) throws IOException;
}
