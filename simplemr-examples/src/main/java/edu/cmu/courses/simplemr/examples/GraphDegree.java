package edu.cmu.courses.simplemr.examples;

import edu.cmu.courses.simplemr.mapreduce.AbstractMapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;

import java.util.Iterator;

/**
 * The graph degree example.
 * count the out-degree and out-degree of every node
 * in a graph data.
 * Every line of input file describe a edge of the graph,
 * in the format:
 * "fromNodeNO. toNodeNO."
 * Output in the format:
 * "nodeNO. out-degree:xxx in-degree:xxx"
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class GraphDegree extends AbstractMapReduce {
    @Override
    public void map(String key, String value, OutputCollector collector) {
        String[] nodes = value.split("\\s+");
        collector.collect(nodes[1], "out-degree");
        collector.collect(nodes[2], "in-degree");
    }

    @Override
    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int outCount = 0;
        int inCount = 0;
        String  value;
        while(values.hasNext()){
            value = values.next();
            if(value.equals("out-degree"))
                outCount++;
            else if(value.equals("in-degree"))
                inCount++;
            else {
                System.out.print("Error Data");
                return;
            }
        }
        collector.collect(key, "out-degree:" + String.valueOf(outCount) +
                " in-degree:" + String.valueOf(inCount));
    }

    public static void main(String[] args) {
        new GraphDegree().run(args);
    }
}
