package edu.cmu.courses.simplemr.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.cmu.courses.simplemr.thrift.ServiceAddress;
import edu.cmu.courses.simplemr.thrift.SlaveWorkload;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class EditLogger {
    private String path;
    private ObjectWriter objectWriter;
    private ObjectMapper objectMapper;
    private BufferedWriter fileWriter;
    private boolean disabled;

    public EditLogger(String path)
            throws IOException {
        this.path = path;
        this.objectMapper = new ObjectMapper();
        this.objectWriter = this.objectMapper.writer();
        this.fileWriter = new BufferedWriter(new FileWriter(path, true));
        this.disabled = false;
    }

    public synchronized void addLog(EditOperation operation)
            throws IOException {
        if(!disabled){
            String log = objectWriter.writeValueAsString(operation);
            fileWriter.append(log);
            fileWriter.newLine();
            fileWriter.flush();
        }
    }

    public synchronized void close()
            throws IOException {
        fileWriter.close();
    }

    public void disable(){
        this.disabled = true;
    }

    public void enable(){
        this.disabled = false;
    }

    public String getPath(){
        return path;
    }
}
