package edu.cmu.courses.simplemr.mapreduce.fileserver;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.BindException;

public class FileServer {
    private static Logger LOG = LoggerFactory.getLogger(FileServer.class);

    private int port;
    private String fileDir;

    public FileServer(int port, String fileDir){
        this.port = port;
        this.fileDir = fileDir;
    }

    public void start()
            throws Exception {
        File folder = new File(fileDir);

        if(!folder.exists()){
            folder.mkdirs();
        }

        if(!folder.isDirectory()){
            throw new IllegalArgumentException("should set directory not a file for file server");
        }

        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new FileHandler(fileDir)), "/*");
        server.setHandler(context);
        try{
            server.start();
        } catch (BindException e){
            LOG.error("bind error, address already in use");
            System.exit(-1);
        }
    }
}
