package edu.cmu.courses.simplemr.dfs;

import edu.cmu.courses.simplemr.Constants;

import java.io.Serializable;

/**
 * The information of a node in distributed file system.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSNode implements Serializable, Comparable<DFSNode> {
    private String serviceName;
    private String registryHost;
    private int registryPort;
    private int chunkNumber;
    private long timestamp;

    public DFSNode(String serviceName, String registryHost, int registryPort){
        this.serviceName = serviceName;
        this.timestamp = 0;
        this.chunkNumber = 0;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public String getRegistryHost() {
        return registryHost;
    }

    public int getRegistryPort() {
        return registryPort;
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
        return getServiceName();
    }

    @Override
    public int hashCode(){
        return getServiceName().hashCode();
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
        return getServiceName().hashCode() - dfsNode.getServiceName().hashCode();
    }
}
