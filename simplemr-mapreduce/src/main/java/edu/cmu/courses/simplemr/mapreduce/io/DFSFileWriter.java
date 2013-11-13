package edu.cmu.courses.simplemr.mapreduce.io;

import edu.cmu.courses.simplemr.dfs.DFSChunk;
import edu.cmu.courses.simplemr.dfs.DFSClient;
import edu.cmu.courses.simplemr.dfs.DFSFile;

import java.io.IOException;

public class DFSFileWriter extends FileWriter {

    private int lineCount;
    private int replicas;
    private DFSClient dfsClient;
    private DFSFile dfsFile;
    private int currentLineCount;
    private StringBuffer lineBuffer;

    public DFSFileWriter(String registryHost, int registryPort,
                         String file, int lineCount, int replicas) {
        super(file);
        this.lineCount = lineCount;
        this.replicas = replicas;
        this.dfsClient = new DFSClient(registryHost, registryPort);
        this.dfsFile = null;
        this.currentLineCount = 0;
        this.lineBuffer = new StringBuffer();
    }

    @Override
    public void open() throws Exception {
        dfsClient.connect();
        dfsFile = dfsClient.createFile(file, replicas);
        if(dfsFile == null){
            throw new IOException("can't create file");
        }
    }

    @Override
    public void close() throws Exception{
        flushChunk();
    }

    @Override
    public void writeLine(String line) throws Exception {
        lineBuffer.append(line + "\n");
        currentLineCount++;
        if(currentLineCount % lineCount == 0){
            flushChunk();
        }
    }

    private void flushChunk() throws IOException {
        int size = lineBuffer.length();
        if(size > 0){
            DFSChunk chunk = dfsClient.createChunk(dfsFile.getId(), 0, size);
            if(chunk == null){
                throw new IOException("can't allocate new chunk");
            }
            if(!dfsClient.writeChunk(chunk, 0, size, lineBuffer.toString().getBytes())){
                throw new IOException("can't write chunk");
            }
            lineBuffer.delete(0, size);
        }
    }
}
