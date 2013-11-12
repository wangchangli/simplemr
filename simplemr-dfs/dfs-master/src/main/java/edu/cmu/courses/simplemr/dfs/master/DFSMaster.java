package edu.cmu.courses.simplemr.dfs.master;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.dfs.DFSConstants;
import edu.cmu.courses.simplemr.dfs.DFSMasterService;

import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DFSMaster {
    @Parameter(names = {"-rh", "--registry-host"}, description = "The host of registry service")
    private String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "The port of registry service")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-l", "--edit-log"}, description = "The path of edit log")
    private String editLogPath = DFSConstants.DEFAULT_MASTER_EDIT_LOG_PATH;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private DFSMasterService service;
    private EditLogger editLogger;
    private DFSMetaData metaData;
    private Registry registry;

    public void start()
            throws IOException {
        editLogger = new EditLogger(editLogPath);
        metaData = new DFSMetaData(editLogger);
        metaData.recoveryFromLog(editLogPath);
        service = new DFSMasterServiceImpl(metaData);
        registry = LocateRegistry.getRegistry(registryHost, registryPort);
        registry.rebind(DFSMasterService.class.getCanonicalName(), service);
    }

    public boolean needHelp(){
        return help;
    }

    public static void main(String[] args) {
        DFSMaster master = new DFSMaster();
        JCommander commander = new JCommander(master, args);
        if(master.needHelp()){
            commander.usage();
        } else {
            try {
                master.start();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
