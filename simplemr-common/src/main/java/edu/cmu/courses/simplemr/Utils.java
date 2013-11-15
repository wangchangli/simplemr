package edu.cmu.courses.simplemr;

import edu.cmu.courses.simplemr.mapreduce.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Defined some often use utilities when doing MapReduce.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class Utils {
    public static String getHost()
            throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        return inetAddress.getHostName();
    }

    public static void postClassFile(String host, int port, Class<?> cls)
            throws IOException {
        InputStream inputStream = cls.getResourceAsStream(cls.getSimpleName() + ".class");

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost post = new HttpPost("http://" + host + ":" + port + "/" + Constants.CLASS_FILE_URI);

        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
        entityBuilder.addBinaryBody(cls.getCanonicalName(), inputStream,
                ContentType.create(Constants.CLASS_CONTENT_TYPE), cls.getCanonicalName());

        HttpEntity entity = entityBuilder.build();
        post.setEntity(entity);

        httpClient.execute(post);
    }

    public static InputStream getRemoteFile(String host, int port, String filePath)
            throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet get = new HttpGet("http://" + host + ":" + port + "/" + filePath);
        HttpResponse response = httpClient.execute(get);
        HttpEntity entity = response.getEntity();
        return entity.getContent();
    }

    public static void validateString(String str, String fieldName){
        if(str == null || str.length() == 0){
            throw new IllegalArgumentException(fieldName + " can't be empty");
        }
    }

    public static void validatePositiveInteger(int number, String fieldName){
        if(number <= 0){
            throw new IllegalArgumentException(fieldName + " can't be negative or zero");
        }
    }

    public static Pair<String, String> splitLine(String line){
        String[] words = line.split(Constants.MAPREDUCE_DELIMITER_REGEX, 2);
        if(words.length < 2){
            return new Pair<String, String>(line, null);
        } else {
            return new Pair<String, String>(words[0], words[1]);
        }
    }

    public static void mergeSortedFiles(List<String> files, String outputFile)
            throws IOException{
        List<BufferedReader> readers = new ArrayList<BufferedReader>();
        Comparator<Pair<String, BufferedReader>> comparator = new Comparator<Pair<String, BufferedReader>>() {
            @Override
            public int compare(Pair<String, BufferedReader> o1, Pair<String, BufferedReader> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        };
        PriorityQueue<Pair<String, BufferedReader>> lineQueues =
                new PriorityQueue<Pair<String, BufferedReader>>(10, comparator);
        for(int i = 0; i < files.size(); i++){
            BufferedReader reader = new BufferedReader(new FileReader(files.get(i)));
            String line = reader.readLine();
            if(line != null){
                lineQueues.add(new Pair<String, BufferedReader>(line, reader));
                readers.add(reader);
            } else {
                reader.close();
            }
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        while(lineQueues.size() > 0){
            Pair<String, BufferedReader> entry = lineQueues.poll();
            writer.write(entry.getKey());
            writer.newLine();
            if(readers.contains(entry.getValue())){
                String line = entry.getValue().readLine();
                if(line != null){
                    lineQueues.offer(new Pair<String, BufferedReader>(line, entry.getValue()));
                } else {
                    entry.getValue().close();
                    readers.remove(entry.getValue());
                }
            }
        }
        writer.flush();
        writer.close();
    }
}
