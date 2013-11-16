package edu.cmu.courses.simplemr.examples;

import edu.cmu.courses.simplemr.mapreduce.AbstractMapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;

import java.util.Iterator;

/**
 * The graph degree example.
 * count the out-degree and in-degree of every node
 * in a graph data.
 * Every line of input file describe a edge of the graph,
 * in the format:
 * "fromNodeNO. toNodeNO."
 * Output in the format:
 * "nodeNO.:xxx out-degree:xxx in-degree:xxx"
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class GraphDegree extends AbstractMapReduce {
    @Override
    public void map(String key, String value, OutputCollector collector) {
        String[] nodes = value.split("\\s+");
        nodes[0] = String.format("%010d", Integer.parseInt(nodes[0]));
        nodes[1] = String.format("%010d", Integer.parseInt(nodes[1]));
        collector.collect(nodes[0], "o");
        collector.collect(nodes[1], "i");
    }

    @Override
    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int outCount = 0;
        int inCount = 0;
        String  value;
        while(values.hasNext()){
            value = values.next();
            if(value.equals("o"))
                outCount++;
            else if(value.equals("i"))
                inCount++;
            else {
                System.out.print("Error Data");
                return;
            }
        }
        collector.collect("nodeNO.:" + key, "out-degree:" + String.valueOf(outCount) +
                " in-degree:" + String.valueOf(inCount));
    }

    public static void main(String[] args) {
        new GraphDegree().run(args);
    }
}
