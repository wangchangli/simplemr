package edu.cmu.courses.simplemr.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EditOperation{
    public static final byte UNKNOWN = 0x0;
    public static final byte SLAVE_REGISTER = 0x1;
    public static final byte SLAVE_REMOVE = 0x2;
    public static final byte DFS_CREATE_FILE = 0x10;
    public static final byte DFS_CREATE_CHUNK = 0x11;
    public static final byte DFS_DELETE_FILE = 0x12;

    private byte type;
    private List<Object> arguments;

    public EditOperation(){
        setType(UNKNOWN);
        setArguments(null);
    }

    public EditOperation(byte type){
        setType(type);
        setArguments(null);
    }

    public EditOperation(byte type, Object[] arguments){
        setType(type);
        setArguments(arguments);
    }

    public void addArgument(Object argument){
        arguments.add(argument);
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public Object[] getArguments() {
        Object[] results = new Object[arguments.size()];
        arguments.toArray(results);
        return results;
    }

    public void setArguments(Object[] arguments) {
        if(arguments == null){
            this.arguments = new ArrayList<Object>();
        } else {
            this.arguments = Arrays.asList(arguments);
        }
    }
}
