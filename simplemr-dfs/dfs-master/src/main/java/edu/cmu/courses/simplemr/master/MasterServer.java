package edu.cmu.courses.simplemr.master;

import edu.cmu.courses.simplemr.Slave;

public class MasterServer {

    private MetaData metaData;

    public void slaveFailure(Slave slave){

    }

    public void slaveSuccess(Slave slave){

    }

    public MetaData getMetaData(){
        return metaData;
    }
}
