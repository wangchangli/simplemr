package edu.cmu.courses.simplemr.dfs;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class DFSFile implements Serializable {
    public static AtomicLong maxId = new AtomicLong(0);

    private long id;
    private String name;
    private Set<Long> chunkIds;
    private int replicas;

    public DFSFile(long id, String name){
        this(id, name, DFSConstants.DEFAULT_REPLICA_NUMBER);
    }

    public DFSFile(long id, String name, int replicas){
        this.id = id;
        this.chunkIds = new TreeSet<Long>();
        setName(name);
        setReplicas(replicas);
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long[] getChunkIds() {
        Long[] ids = new Long[chunkIds.size()];
        chunkIds.toArray(ids);
        return ids;
    }

    public void addChunkId(long chunkId){
        chunkIds.add(chunkId);
    }

    public void removeChunkId(long chunkId){
        chunkIds.remove(chunkId);
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }
}
