
package edu.cmu.courses.simplemr.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;


public class LocalFileReader extends FileReader{

    BufferedReader reader;

    public LocalFileReader(FileBlock fileBlock) throws FileNotFoundException {
        super(fileBlock);
        reader = new BufferedReader(new java.io.FileReader(fileBlock.getFile()));
    }

    @Override
    public void open() throws Exception{


    }

    @Override
    public void close(){
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public String readLine() throws Exception {
        String line = reader.readLine();
        return line;
    }
}