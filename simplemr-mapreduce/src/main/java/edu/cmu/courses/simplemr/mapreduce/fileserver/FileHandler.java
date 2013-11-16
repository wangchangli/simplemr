package edu.cmu.courses.simplemr.mapreduce.fileserver;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.common.MapReduceConstants;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLConnection;
import java.util.List;

/**
 * The handler of files that transform from mappers to reducers.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class FileHandler extends HttpServlet {
    private String baseDir;
    private DiskFileItemFactory factory;
    private ServletFileUpload uploadHandler;

    public FileHandler(String baseDir){
        this.baseDir = baseDir;
        this.factory = new DiskFileItemFactory();
        this.uploadHandler = new ServletFileUpload(factory);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String filePath = baseDir + request.getRequestURI().replaceAll("/", Constants.FILE_SEPARATOR);
        File file = new File(filePath);
        if(!file.exists()){
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        if(file.isDirectory()){
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        response.setContentType(URLConnection.guessContentTypeFromName(filePath));

        FileInputStream in = new FileInputStream(file);
        OutputStream out = response.getOutputStream();
        IOUtils.copy(in, out);
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        if(!ServletFileUpload.isMultipartContent(request)){
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        try {
            List<FileItem> files = uploadHandler.parseRequest(request);
            if(files.size() == 0){
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            FileItem fileItem = files.get(0);
            if(!fileItem.getContentType().equals(Constants.CLASS_CONTENT_TYPE) || fileItem.isFormField()){
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            String folderName = baseDir + request.getRequestURI().replaceAll("/", Constants.FILE_SEPARATOR);
            File folder = new File(folderName);
            if(folder.exists() && !folder.isDirectory()){
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            if(!folder.exists()){
                folder.mkdirs();
            }
            fileItem.write(new File(folderName + Constants.FILE_SEPARATOR + fileItem.getName()));
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }
    }
}
