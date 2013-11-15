package edu.cmu.courses.simplemr.mapreduce.task;

import java.io.Serializable;
/**
 * Status of a task.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */
public enum TaskStatus implements Serializable{
    PENDING, FAILED, SUCCEED
}
