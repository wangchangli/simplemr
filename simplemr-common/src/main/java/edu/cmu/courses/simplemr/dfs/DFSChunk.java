package edu.cmu.courses.simplemr.dfs;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class DFSChunk implements Serializable, Comparable<DFSChunk> {
    public static AtomicLong maxId = new AtomicLong(0);

    private long id;
    private long fileId;
    private int offset;
    private int size;
    private Set<DFSNode> nodes;

    public DFSChunk(long id, long fileId, int offset, int size, DFSNode[] nodes){
        this.id = id;
        this.fileId = fileId;
        this.offset = offset;
        this.size = size;
        this.nodes = new TreeSet<DFSNode>(Arrays.asList(nodes));
    }

    public long getId() {
        return id;
    }

    public long getFileId() {
        return fileId;
    }

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public void addNode(DFSNode node){
        nodes.add(node);
    }

    public void removeNode(DFSNode node){
        nodes.remove(node);
    }

    public DFSNode[] getNodes(){
        DFSNode[] nodesArray = new DFSNode[nodes.size()];
        nodes.toArray(nodesArray);
        return nodesArray;
    }

    @Override
    public int compareTo(DFSChunk dfsChunk) {
        return (int)(id - dfsChunk.getId());
    }
}
