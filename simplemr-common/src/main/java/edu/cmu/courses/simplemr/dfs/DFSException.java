package edu.cmu.courses.simplemr.dfs;

import java.rmi.RemoteException;

public class DFSException extends RemoteException {
    public DFSException(String message) {
        super(message);
    }
}
