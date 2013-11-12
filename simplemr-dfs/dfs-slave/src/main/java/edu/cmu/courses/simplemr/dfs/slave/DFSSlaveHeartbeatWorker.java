package edu.cmu.courses.simplemr.dfs.slave;

import edu.cmu.courses.simplemr.dfs.DFSMasterService;
import edu.cmu.courses.simplemr.dfs.DFSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public class DFSSlaveHeartbeatWorker implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(DFSSlaveHeartbeatWorker.class);

    private DFSSlave slave;
    private DFSMasterService masterService;

    public DFSSlaveHeartbeatWorker(DFSSlave slave, DFSMasterService masterService){
        this.masterService = masterService;
        this.slave = slave;
    }

    @Override
    public void run() {
        try {
            masterService.heartbeat(slave.getServiceName(), slave.getChunkNumber());
        } catch (RemoteException e) {
            LOG.error("master node error", e);
        }
    }
}
