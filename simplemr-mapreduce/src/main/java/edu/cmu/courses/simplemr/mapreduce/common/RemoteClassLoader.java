package edu.cmu.courses.simplemr.mapreduce.common;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Load the .class file using HTTP
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class RemoteClassLoader extends ClassLoader {
    public Class<?> loadRemoteClass(String host, int port, String className)
            throws IOException, ClassNotFoundException {
        InputStream in = Utils.getRemoteFile(host, port, Constants.CLASS_FILE_URI + "/" + className);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        byte[] classBytes = out.toByteArray();
        out.close();
        in.close();
        return defineClass(className, classBytes, 0, classBytes.length);
    }
}
