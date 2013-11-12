package edu.cmu.courses.simplemr.dfs.slave;

import edu.cmu.courses.simplemr.dfs.DFSMasterService;
import edu.cmu.courses.simplemr.dfs.DFSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

public class DFSSlaveHeartbeatWorker implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(DFSSlaveHeartbeatWorker.class);

    private DFSSlave slave;
    private Registry registry;

    public DFSSlaveHeartbeatWorker(DFSSlave slave, Registry registry){
        this.registry = registry;
        this.slave = slave;
    }

    @Override
    public void run() {
        try {
            DFSMasterService masterService = (DFSMasterService)
                    registry.lookup(DFSMasterService.class.getCanonicalName());
            masterService.heartbeat(slave.getServiceName(), slave.getChunkNumber());
        } catch (RemoteException e) {
            LOG.error("master node error", e);
        } catch (NotBoundException e) {
            LOG.error("master service not found", e);
        }
    }
}
