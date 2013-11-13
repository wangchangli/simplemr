package edu.cmu.courses.simplemr.mapreduce.task;

public class ReducerTask extends Task {

    private String outputFilePrefix;
    String mapperTrackerHost;
    int mapperTrackerPort;
    int mapperTaskId;
    String reducerInputDir;
    boolean startReduce;

    /**
     * which partition of file to get from a mapper
     */
    int partition;



    //private List<MapperTask> mapperTasks;

    public ReducerTask(int taskId, int jobId) {
        super(taskId, jobId, TaskType.REDUCER);
    }

    public void setMapperTrackerHost(String host) {
        this.mapperTrackerHost = host;
    }

    public String getMapperTrackerHost() {
        return mapperTrackerHost;
    }

    public void setMapperTrackerPort(int port) {
        this.mapperTrackerPort = port;
    }

    public int getMapperTrackerPort() {
        return mapperTrackerPort;
    }

    public void setMapperTaskId(int id) {
        mapperTaskId = id;
    }

    public int getMapperTaskId() {
        return mapperTaskId;
    }

    public void setReducerInputDir(String dir) {
        reducerInputDir = dir;
    }

    public String getReducerInputDir() {
        return reducerInputDir;
    }

    public int getPartition(){
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setStartReduce(boolean b){
        this.startReduce = b;
    }

    public boolean getStartReduce() {
        return startReduce;
    }
}
