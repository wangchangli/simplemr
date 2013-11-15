package edu.cmu.courses.simplemr.mapreduce.task;

import java.io.Serializable;

/**
 * Type of task.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public enum TaskType implements Serializable{
    MAPPER,REDUCER
}
