package edu.cmu.courses.simplemr.master;

import edu.cmu.courses.simplemr.Slave;
import edu.cmu.courses.simplemr.thrift.SlaveService;
import edu.cmu.courses.simplemr.thrift.SlaveWorkload;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatWorker implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(HeartbeatWorker.class);

    private Slave slave;
    private MasterServer masterServer;

    public HeartbeatWorker(MasterServer masterServer, Slave slave){
        this.masterServer = masterServer;
        this.slave = slave;
    }

    @Override
    public void run() {
        try{
            TBinaryProtocol protocol = new TBinaryProtocol(new TFramedTransport(
                    new TSocket(slave.getLocation().getHost(), slave.getLocation().getPort())));
            SlaveService.Client slaveClient = new SlaveService.Client(protocol);
            SlaveWorkload workload = slaveClient.heartbeat();
            slave.setWorkload(workload);
            masterServer.slaveSuccess(slave);
        } catch (Exception e){
            LOG.warn("slave " + slave + " heartbeat failure", e);
            masterServer.slaveFailure(slave);
        }
    }
}
