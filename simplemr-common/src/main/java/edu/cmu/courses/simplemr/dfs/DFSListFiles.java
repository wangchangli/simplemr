package edu.cmu.courses.simplemr.dfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * The "dfs-ls" operation in distributed file system.
 * List all the files in a directory.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSListFiles {
    @Parameter(names = {"-rh", "--registry-host"}, description = "The host of registry service")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "The port of registry service")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    public boolean needHelp(){
        return help;
    }

    public void listFiles()
            throws RemoteException, NotBoundException {
        DFSClient dfsClient = new DFSClient(registryHost, registryPort);
        dfsClient.connect();
        String[] files = dfsClient.listFiles();
        if(files.length > 0){
            for(String file : files){
                System.out.println(file);
            }
        } else {
            System.out.println("--- No files here ---");
        }
    }

    public static void main(String[] args)
            throws RemoteException, NotBoundException {
        DFSListFiles listFiles = new DFSListFiles();
        JCommander commander = new JCommander(listFiles, args);
        commander.setProgramName("dfs-ls");
        if(listFiles.needHelp()){
            commander.usage();
        } else {
            listFiles.listFiles();
        }
    }
}
