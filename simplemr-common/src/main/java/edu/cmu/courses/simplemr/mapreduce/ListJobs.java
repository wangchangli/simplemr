package edu.cmu.courses.simplemr.mapreduce;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.cmu.courses.simplemr.Constants;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * List all the jobs.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class ListJobs {
    @Parameter(names = {"-rh", "--registry-host"}, description = "the registry host")
    protected String registryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-rp", "--registry-port"}, description = "the registry port")
    protected int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help = false;

    public boolean needHelp(){
        return help;
    }

    public String getRegistryHost() {
        return registryHost;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public static void main(String[] args) throws RemoteException, NotBoundException {
        ListJobs listJobs = new ListJobs();
        JCommander commander = new JCommander(listJobs, args);
        commander.setProgramName("mapreduce-jobs");
        if(listJobs.needHelp()){
            commander.usage();
        } else {
            Registry registry = LocateRegistry.getRegistry(listJobs.getRegistryHost(), listJobs.getRegistryPort());
            JobClientService service = (JobClientService)registry.lookup(JobClientService.class.getCanonicalName());
            System.out.println(service.describeJobs());
        }
    }
}
