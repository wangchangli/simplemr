package edu.cmu.courses.simplemr.dfs;

import java.rmi.RemoteException;

/**
 * Exceptions happen in distributed file system
 * are also a Remote Exception in RMI.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSException extends RemoteException {
    public DFSException(String message) {
        super(message);
    }
}
