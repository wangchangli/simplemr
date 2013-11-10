package edu.cmu.courses.simplemr;

import edu.cmu.courses.simplemr.thrift.ServiceAddress;
import edu.cmu.courses.simplemr.thrift.SlaveWorkload;

public class Slave {
    private long id;
    private ServiceAddress location;
    private SlaveWorkload workload;

    public Slave(long id, ServiceAddress location){
        setId(id);
        setLocation(location);
        setWorkload(new SlaveWorkload());
    }

    public Slave(long id, ServiceAddress location, SlaveWorkload workload){
        setId(id);
        setLocation(location);
        setWorkload(workload);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ServiceAddress getLocation() {
        return location;
    }

    public void setLocation(ServiceAddress location) {
        this.location = location;
    }

    public SlaveWorkload getWorkload() {
        return workload;
    }

    public void setWorkload(SlaveWorkload workload) {
        this.workload = workload;
    }

    public String toString(){
        return locationToString(location);
    }

    public static String locationToString(ServiceAddress location){
        return location.host + ":" + location.port;

    }
}
