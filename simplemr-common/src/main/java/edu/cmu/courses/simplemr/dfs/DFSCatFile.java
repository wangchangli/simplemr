package edu.cmu.courses.simplemr.dfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

/**
 * The "dfs-cat" operation in distributed file system.
 * Showing the file.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSCatFile {
    @Parameter(required = true, description = "filename")
    private List<String> fileNames;

    @Parameter(names = {"-rh", "--master-registry-host"}, description = "The host of master registry service")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--master-registry-port"}, description = "The port of master registry service")
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
        if(file == null){
            System.out.println("No such file");
            return;
        }
        DFSChunk[] chunks = file.getChunks();
        for(DFSChunk chunk : chunks){
            int offset = 0;
            int blockSize = 4096;
            byte[] data = null;
            while((data = dfsClient.readChunk(chunk, offset, blockSize)) != null){
                System.out.print(new String(data));
                offset += blockSize;
            }
        }
    }

    public static void main(String[] args)
            throws RemoteException, NotBoundException {
        DFSCatFile catFile = new DFSCatFile();
        JCommander commander = new JCommander(catFile, args);
        commander.setProgramName("dfs-cat");
        if(catFile.needHelp()){
            commander.usage();
        } else {
            catFile.catFile();
        }
    }
}
