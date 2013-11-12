package edu.cmu.courses.simplemr.dfs;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class DFSFile implements Serializable {
    public static AtomicLong maxId = new AtomicLong(0);

    private long id;
    private String name;
    private Set<DFSChunk> chunks;
    private int replicas;

    public DFSFile(long id, String name){
        this(id, name, DFSConstants.DEFAULT_REPLICA_NUMBER);
    }

    public DFSFile(long id, String name, int replicas){
        this.id = id;
        this.chunks = new TreeSet<DFSChunk>();
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

    public DFSChunk[] getChunks() {
        DFSChunk[] chunks = new DFSChunk[this.chunks.size()];
        this.chunks.toArray(chunks);
        Arrays.sort(chunks, new Comparator<DFSChunk>() {
            @Override
            public int compare(DFSChunk dfsChunk, DFSChunk dfsChunk2) {
                return dfsChunk.getOffset() - dfsChunk2.getOffset();
            }
        });
        return chunks;
    }

    public void addChunk(DFSChunk chunk){
        chunks.add(chunk);
    }

    public void removeChunkId(DFSChunk chunk){
        chunks.remove(chunk);
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }
}
