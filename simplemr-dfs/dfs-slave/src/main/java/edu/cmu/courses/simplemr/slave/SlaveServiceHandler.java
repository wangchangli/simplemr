package edu.cmu.courses.simplemr.slave;

import edu.cmu.courses.simplemr.thrift.SlaveService;
import edu.cmu.courses.simplemr.thrift.SlaveWorkload;
import org.apache.thrift.TException;

public class SlaveServiceHandler implements SlaveService.Iface{
    @Override
    public SlaveWorkload heartbeat() throws TException {
        return null;  //TODO needs implementation.
    }
}
