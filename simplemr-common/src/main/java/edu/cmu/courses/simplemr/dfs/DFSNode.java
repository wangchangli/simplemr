package edu.cmu.courses.simplemr.dfs;

import java.io.Serializable;

public class DFSNode implements Serializable {
    private String serviceName;
    private String host;
    private int dataPort;
    private int chunkNumber;
    private long timestamp;

    public DFSNode(String serviceName){
        this(serviceName, null, -1);
    }

    public DFSNode(String host, int dataPort){
        this(convertServiceName(host, dataPort), host, dataPort);
    }

    public DFSNode(String serviceName, String host, int dataPort){
        if(dataPort <= 0){
            this.dataPort = DFSConstants.DEFAULT_DATA_PORT;
        } else {
            this.dataPort = dataPort;
        }
        this.host = host;
        this.serviceName = serviceName;
        this.timestamp = 0;
        this.chunkNumber = 0;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getHost() {
        return host;
    }

    public int getDataPort() {
        return dataPort;
    }

    public int getChunkNumber(){
        return chunkNumber;
    }

    public void setChunkNumber(int chunkNumber){
        this.chunkNumber = chunkNumber;
    }

    public static String convertServiceName(String host, int dataPort){
        return host + ":" + dataPort;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return serviceName;
    }

    @Override
    public int hashCode(){
        return serviceName.hashCode();
    }

    @Override
    public boolean equals(Object node){
        if(node instanceof DFSNode){
            return hashCode() == node.hashCode();
        } else {
            return false;
        }
    }
}
