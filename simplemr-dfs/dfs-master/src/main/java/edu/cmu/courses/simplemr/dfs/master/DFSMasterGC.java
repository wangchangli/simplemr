package edu.cmu.courses.simplemr.dfs.master;

import edu.cmu.courses.simplemr.dfs.DFSChunk;
import edu.cmu.courses.simplemr.dfs.DFSNode;
import edu.cmu.courses.simplemr.dfs.DFSSlaveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Runnable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DFSMasterGC implements Runnable{
    private static Logger LOG = LoggerFactory.getLogger(DFSMasterGC.class);

    private DFSChunk chunk;

    public DFSMasterGC(DFSChunk chunk){
        this.chunk = chunk;
    }

    @Override
    public void run() {
        DFSNode[] nodes = chunk.getNodes();
        try {
            for(DFSNode node : nodes){
                Registry registry = LocateRegistry.getRegistry(node.getRegistryHost(), node.getRegistryPort());
                DFSSlaveService slaveService = (DFSSlaveService) registry.lookup(node.getServiceName());
                slaveService.delete(chunk.getId());
            }
        } catch (RemoteException e) {
            LOG.error("rmi error for registry or slave", e);
        } catch (NotBoundException e) {
            LOG.error("service not bound", e);
        }
    }
}
