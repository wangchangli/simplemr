package edu.cmu.courses.simplemr.dfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.List;

/**
 * The "dfs-load" operation in distributed file system.
 * Load a file from local file system sto DFS.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSLoadFile {
    @Parameter(required = true, description = "filename")
    private List<String> fileNames;

    @Parameter(names = {"-t", "--text"}, description = "Set this if the file is a text file")
    private boolean isText = false;

    @Parameter(names = {"-rh", "--registry-host"}, description = "The host of registry service")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "The port of registry service")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-r", "--replica"}, description = "The number of replicas")
    private int replicas = DFSConstants.DEFAULT_REPLICA_NUMBER;

    @Parameter(names = {"-b", "--block"}, description = "The size of every block")
    private int blockSize = DFSConstants.DEFAULT_BLOCK_SIZE;

    @Parameter(names = {"-l", "--lines"}, description = "The line count of every block")
    private int lineCount = DFSConstants.DEFAULT_LINE_COUNT;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    public boolean needHelp(){
        return help;
    }

    public void loadFile()
            throws IOException, NotBoundException {
        DFSClient dfsClient = new DFSClient(registryHost, registryPort);
        dfsClient.connect();
        boolean success = false;
        if(isText){
            success = dfsClient.writeText(fileNames.get(0), replicas, lineCount);
        } else {
            success = dfsClient.write(fileNames.get(0), replicas, blockSize);
        }
        if(!success){
            System.out.println("failed to write file");
        }
    }

    public static void main(String[] args) throws IOException, NotBoundException {
        DFSLoadFile loadFile = new DFSLoadFile();
        JCommander commander = new JCommander(loadFile, args);
        commander.setProgramName("dfs-load");
        if(loadFile.needHelp()){
            commander.usage();
        } else {
            loadFile.loadFile();
        }
    }
}
