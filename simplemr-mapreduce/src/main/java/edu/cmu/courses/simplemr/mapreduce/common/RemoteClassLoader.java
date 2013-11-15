package edu.cmu.courses.simplemr.mapreduce.common;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class RemoteClassLoader extends ClassLoader {
    public void loadRemoteClass(String host, int port, String className)
            throws IOException {
        InputStream in = Utils.getRemoteFile(host, port, Constants.CLASS_FILE_URI + "/" + className);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        byte[] classBytes = out.toByteArray();
        defineClass(className, classBytes, 0, classBytes.length);
        out.close();
    }
}
