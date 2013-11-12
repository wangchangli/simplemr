package edu.cmu.courses.simplemr.dfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

public class DFSCatFile {
    @Parameter(required = true, description = "filename")
    private List<String> fileNames;

    @Parameter(names = {"-rh", "--registry-host"}, description = "The host of registry service")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "The port of registry service")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    public boolean needHelp(){
        return help;
    }

    public void catFile()
            throws RemoteException, NotBoundException {
        DFSClient dfsClient = new DFSClient(registryHost, registryPort);
        dfsClient.connect();
        DFSFile file = dfsClient.getFile(fileNames.get(0));
        DFSChunk[] chunks = file.getChunks();
        for(DFSChunk chunk : chunks){
            int offset = 0;
            int blockSize = 4096;
            byte[] data = null;
            while((data = dfsClient.readChunk(chunk, offset, blockSize)) != null){
                System.out.println(new String(data));
                offset += blockSize;
            }
        }
    }

    public static void main(String[] args)
            throws RemoteException, NotBoundException {
        DFSCatFile catFile = new DFSCatFile();
        JCommander commander = new JCommander(catFile, args);
        if(catFile.needHelp()){
            commander.usage();
        } else {
            catFile.catFile();
        }
    }
}
