package edu.cmu.courses.mapreduce.task;

import java.io.Serializable;

public enum TaskStatus implements Serializable{
    INITIALIZING, WAITING, PENDING, FAILED, SUCCEED
}
