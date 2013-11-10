package edu.cmu.courses.simplemr.master;

import edu.cmu.courses.simplemr.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class HeartbeatDispatcher implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(HeartbeatDispatcher.class);

    private MasterServer masterServer;
    private ExecutorService pool;
    private long heartBeatPeriod;

    public HeartbeatDispatcher(MasterServer masterServer, int threadPoolSize, long heartBeatPeriod){
        this.masterServer = masterServer;
        this.pool = Executors.newFixedThreadPool(threadPoolSize);
        this.heartBeatPeriod = heartBeatPeriod;
    }

    @Override
    public void run() {
        while(true){
            MetaData metaData = masterServer.getMetaData();
            Slave[] slaves = metaData.getSlaves();
            for(Slave slave : slaves){
                pool.execute(new HeartbeatWorker(masterServer, slave));
            }
            try {
                Thread.sleep(heartBeatPeriod);
            } catch (InterruptedException e) {
                LOG.error("heartbeat dispatcher is interrupted", e);
                break;
            }
        }
    }
}
