package edu.cmu.courses.simplemr.mapreduce.task;

import java.io.Serializable;

public enum TaskStatus implements Serializable{
    INITIALIZING, WAITING, PENDING, FAILED, SUCCEED
}
