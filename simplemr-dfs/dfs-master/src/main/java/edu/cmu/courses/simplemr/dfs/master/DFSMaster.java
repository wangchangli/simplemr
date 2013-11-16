package edu.cmu.courses.simplemr.dfs.master;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.dfs.DFSConstants;
import edu.cmu.courses.simplemr.dfs.DFSMasterService;

import java.io.File;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * The DFS Master class. DFS master stores the metadata that
 * user use to retrieve data in DFS nodes. The log of master is
 * stored in logger, so the master can be recovered after dead.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSMaster {
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
        if(new File(editLogPath).isDirectory()){
            throw new IllegalArgumentException("Log path should be a file, not directory");
        }
        editLogger = new EditLogger(editLogPath);
        metaData = new DFSMetaData(this, editLogger);
        metaData.recoveryFromLog(editLogPath);
        service = new DFSMasterServiceImpl(metaData);
        registry = LocateRegistry.getRegistry(Utils.getHost(), registryPort);
        registry.rebind(DFSMasterService.class.getCanonicalName(), service);
    }

    public boolean needHelp(){
        return help;
    }

    public int getRegistryPort(){
        return registryPort;
    }

    public static void main(String[] args) {
        DFSMaster master = new DFSMaster();
        JCommander commander = new JCommander(master, args);
        commander.setProgramName("dfs-master");
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
