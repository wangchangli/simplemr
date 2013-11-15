package edu.cmu.courses.simplemr.dfs.slave;

import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.dfs.DFSMasterService;
import edu.cmu.courses.simplemr.dfs.DFSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

public class DFSSlaveHeartbeatWorker implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(DFSSlaveHeartbeatWorker.class);

    private DFSSlave slave;
    private Registry masterRegistry;

    public DFSSlaveHeartbeatWorker(DFSSlave slave, Registry masterRegistry){
        this.masterRegistry = masterRegistry;
        this.slave = slave;
    }

    @Override
    public void run() {
        try {
            DFSMasterService masterService = (DFSMasterService)
                    masterRegistry.lookup(DFSMasterService.class.getCanonicalName());
            masterService.heartbeat(slave.getServiceName(), Utils.getHost(), slave.getRegistryPort(),
                    slave.getChunkNumber());
        } catch (RemoteException e) {
            LOG.error("master node error", e);
        } catch (NotBoundException e) {
            LOG.error("master service not found", e);
        } catch (UnknownHostException e) {
            LOG.error("can't resolve hostname");
        }
    }
}
