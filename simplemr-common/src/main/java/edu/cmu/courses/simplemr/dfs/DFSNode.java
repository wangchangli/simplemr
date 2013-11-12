package edu.cmu.courses.simplemr.dfs;

import edu.cmu.courses.simplemr.Constants;

import java.io.Serializable;

public class DFSNode implements Serializable, Comparable<DFSNode> {
    private String serviceName;
    private int chunkNumber;
    private long timestamp;

    public DFSNode(String serviceName){
        this.serviceName = serviceName;
        this.timestamp = 0;
        this.chunkNumber = 0;
    }

    public String getServiceName() {
        return serviceName;
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

    public boolean isValid(){
        return System.currentTimeMillis() - timestamp < Constants.DEFAULT_HEARTBEAT_INVALID;
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

    @Override
    public int compareTo(DFSNode dfsNode) {
        return serviceName.hashCode() - dfsNode.getServiceName().hashCode();
    }
}
