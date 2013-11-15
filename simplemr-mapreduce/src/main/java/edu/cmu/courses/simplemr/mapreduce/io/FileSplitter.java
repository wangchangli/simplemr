package edu.cmu.courses.simplemr.mapreduce.io;

import java.util.List;

/**
 * The split file interface.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public interface FileSplitter {
    public List<FileBlock> split(String file, int number) throws Exception;
}
