package edu.cmu.courses.simplemr.io;


import java.io.BufferedWriter;
import java.io.IOException;

public class LocalFileWriter extends FileWriter {
    BufferedWriter writer;
    public LocalFileWriter(String fileName) throws IOException {
        super(fileName);
        writer = new BufferedWriter(new java.io.FileWriter(fileName));
    }

    @Override
    public void open() throws Exception{

    }

    @Override
    public void close() throws Exception {
        writer.close();
    }

    @Override
    public void writeLine(String line) throws Exception {
        writer.write(line + "\n");
    }

}