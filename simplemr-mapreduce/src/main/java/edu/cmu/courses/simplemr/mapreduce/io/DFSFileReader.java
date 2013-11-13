package edu.cmu.courses.simplemr.mapreduce.io;

import edu.cmu.courses.simplemr.dfs.DFSChunk;
import edu.cmu.courses.simplemr.dfs.DFSClient;
import edu.cmu.courses.simplemr.dfs.DFSFile;

import java.io.IOException;

public class DFSFileReader extends FileReader {

    private DFSClient dfsClient;
    private DFSFile dfsFile;
    private long currentOffset;
    private StringBuffer chunkBuffer;

    public DFSFileReader(String registryHost, int registryPort, FileBlock fileBlock) {
        super(fileBlock);
        this.dfsClient = new DFSClient(registryHost, registryPort);
        this.chunkBuffer = new StringBuffer();
        this.currentOffset = fileBlock.getOffset();
    }

    @Override
    public void open() throws Exception {
        dfsClient.connect();
        dfsFile = dfsClient.getFile(fileBlock.getFile());
        if(dfsFile == null){
            throw new IOException("can't get file metadata");
        }
    }

    @Override
    public void close() {

    }

    @Override
    public String readLine() throws Exception {
        StringBuffer lineBuffer = new StringBuffer();
        while(true){
            if(fileBlock.getSize() >= 0 && currentOffset == fileBlock.getOffset() + fileBlock.getSize()){
                break;
            }
            if(chunkBuffer.length() == 0){
                DFSChunk chunk = getNextChunk();
                if(chunk == null){
                    break;
                }
                long chunkOffset = currentOffset - chunk.getOffset();
                int chunkSize = 0;
                if(fileBlock.getSize() < 0){
                    chunkSize = (int) (chunk.getSize() - chunkOffset);
                } else {
                    chunkSize = (int)(Math.min(fileBlock.getOffset() + fileBlock.getSize(),
                                               chunk.getOffset() + chunk.getSize()) - currentOffset);
                }
                byte[] data = dfsClient.readChunk(chunk, chunkOffset, chunkSize);
                if(data == null || data.length == 0){
                    break;
                }
                chunkBuffer.append(new String(data));
            }
            char ch = chunkBuffer.charAt(0);
            chunkBuffer.deleteCharAt(0);
            currentOffset++;
            lineBuffer.append(ch);
            if(ch == '\n'){
                break;
            }
        }

        if(lineBuffer.length() == 0){
            return null;
        } else {
            if(lineBuffer.charAt(lineBuffer.length() - 1) == '\n'){
                lineBuffer.deleteCharAt(lineBuffer.length() - 1);
            }
            return lineBuffer.toString();
        }
    }

    private DFSChunk getNextChunk(){
        for(DFSChunk chunk : dfsFile.getChunks()){
            if(chunk.getOffset() <= currentOffset &&
               chunk.getOffset() + chunk.getSize() > currentOffset){
                return chunk;
            }
        }
        return null;
    }
}
