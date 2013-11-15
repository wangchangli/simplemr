package edu.cmu.courses.simplemr.dfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

public class DFSRemoveFile {
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

    public void remove()
            throws RemoteException, NotBoundException {
        DFSClient dfsClient = new DFSClient(registryHost, registryPort);
        dfsClient.connect();
        dfsClient.deleteFile(fileNames.get(0));
    }

    public static void main(String[] args) throws RemoteException, NotBoundException {
        DFSRemoveFile removeFile = new DFSRemoveFile();
        JCommander commander = new JCommander(removeFile, args);
        commander.setProgramName("dfs-rm");
        if(removeFile.needHelp()){
            commander.usage();
        } else {
            removeFile.remove();
        }
    }
}
