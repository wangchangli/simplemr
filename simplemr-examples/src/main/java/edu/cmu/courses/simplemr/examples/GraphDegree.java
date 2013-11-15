package edu.cmu.courses.simplemr.examples;

import edu.cmu.courses.simplemr.mapreduce.AbstractMapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;

import java.util.Iterator;

/**
 * The graph degree example.
 * count the out-degree and out-degree of every node
 * in a graph dataset.
 * Each line of input is a pair of number:
 * "fromNodeNO. toNodeNO."
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class GraphDegree extends AbstractMapReduce {
    @Override
    public void map(String key, String value, OutputCollector collector) {
        String[] nodes = value.split("\\s+");
        collector.collect(nodes[0] + "-out-degree", "1");
        collector.collect(nodes[1] + "-in-degree", "1");
    }

    @Override
    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int count = 0;
        while(values.hasNext()){
            count++;
            values.next();
        }
        collector.collect(key, String.valueOf(count));
    }

    public static void main(String[] args) {
        new GraphDegree().run(args);
    }
}
