package edu.cmu.courses.simplemr.mapreduce.io;

import edu.cmu.courses.simplemr.dfs.DFSClient;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * Split a file in distributed file system.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSFileSplitter implements FileSplitter {

    private DFSClient dfsClient;

    public DFSFileSplitter(String registryHost, int registryPort)
            throws RemoteException, NotBoundException {
        dfsClient = new DFSClient(registryHost, registryPort);
        dfsClient.connect();
    }

    @Override
    public List<FileBlock> split(String file, int number) throws Exception {
        List<FileBlock> blocks = new ArrayList<FileBlock>();
        long[] offsets = dfsClient.linesOffset(file);
        if(offsets == null){
            return blocks;
        }
        int rangeCount = Math.min(number, offsets.length);
        int rangeSize = offsets.length / rangeCount;
        for(int i = 0; i < rangeCount; i++){
            long offset = offsets[i * rangeSize];
            int size = -1;
            if(i < rangeCount - 1){
                size = (int) (offsets[(i + 1) * rangeSize] - offset);
            }
            blocks.add(new FileBlock(file, offset, size));
        }
        return blocks;
    }
}
