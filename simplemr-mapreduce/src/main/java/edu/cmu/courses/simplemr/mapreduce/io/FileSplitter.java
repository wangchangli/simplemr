package edu.cmu.courses.simplemr.mapreduce.io;

import java.util.List;

public interface FileSplitter {
    public List<FileBlock> split(String file, int number);
}
