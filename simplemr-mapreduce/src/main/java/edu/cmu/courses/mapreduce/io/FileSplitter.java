package edu.cmu.courses.mapreduce.io;

import java.util.List;

public interface FileSplitter {
    public List<FileBlock> split(String file, int number);
}
