###SimpleMR - Simple Implementation of MapReduce

####Overview

SimpleMR is a lightweight [MapReduce](http://research.google.com/archive/mapreduce-osdi04.pdf) framework.
SimpleMR contains two parts: the MapReduce computation model and a Distributed File System. The design concepts of DFS
is largely come from [Google File System](http://research.google.com/archive/gfs-sosp2003.pdf).
Here are the components of SimpleMR:

* `simplemr-dfs` - the implementation of Distributed File System

* `simplemr-mapreduce` - the implementation of MapReduce computation model

* `simplemr-common` - the programming library of SimpleMR

* `simplemr-examples` - the examples

####How to build

Our project is managed by [Apache Maven](http://maven.apache.org/). In order to build our project, you should install
maven first. After successfully installing maven, you can build our project as below:

    $ mvn package

####How to run

Before you start, you should grant the execute permission of `dist/bin/*`:

    $ chmod +x dist/bin/*

The communication framework of our project is [Java RMI](http://en.wikipedia.org/wiki/RMI), so please make sure the
`/usr/bin/rmiregistry` is executable in your machine. Here's the command to start Java RMI registry server:

    $ dist/bin/registry PORT &

**Note**: If you want to run our project on a cluster, you should start registry server on **every single node**, since
Java RMI don't support remote binding object.

Now we need to start the Distributed File System. `simplemr-dfs` contains two components: `dfs-master` and
`dfs-slave`. `dfs-master` is designed to save metadata of file, whereas `dfs-slave` is designed to save *real* data of
file. To run DFS, you should first start the `dfs-master`, here is the command:

    $ dist/bin/dfs-master -l LOG_PATH -rp REGISTRY_PORT &

Please make sure your `registry` server is running on the same machine, for more information, please use `-h`  or
`--help` option.

Then you need to start the `dfs-slave`, in order to run multiple slaves on a single machine (just for test), you need
to give every slave a *different* service name. Here is the command to start `dfs-slave`

    $ dist/bin/dfs-slave \
    -d DATA_DIR \
    -mh MASTER_REGISTRY_HOST \
    -mp MASTER_REGISTRY_PORT \
    -rp LOCAL_REGISTRY_PORT \
    -n SLAVE_NAME &

Please make sure your `registry` server is running on the same machine, for more information, please use `-h`  or
`--help` option.

We also offered several tools for querying, loading and deleting the files in DFS, it contains:

* `dist/bin/dfs-ls` - list files of the DFS
* `dist/bin/dfs-load` - load file into the DFS
* `dist/bin/dfs-cat` - read file from the DFS
* `dist/bin/dfs-rm` - remove file in the DFS

You can use `-h` or `--help` to see more information on each command

The next step is starting the MapReduce computation framework, this components contains two parts:
`mapreduce-jobtracker` and `mapreduce-tasktracker`. `mapreduce-jobtracker` is designed to communicate with client,
`mapreduce-tasktracker` and schedule the MapReduce job. It has *only one* instance in the whole environment
The `mapreduce-tasktracker` has multiple instances, it runs on different nodes in a cluster. It tracks the tasks
running on that node.

Now let's start the `mapreduce-jobtracker`, here's the command:

    $ dist/bin/mapreduce-jobtracker \
    -dh DFS_MASTER_REGISTRY_HOST \
    -dp DFS_MASTER_REGISTRY_PORT \
    -rp LOCAL_REGISTRY_PORT \
    -fp FILE_SERVER_PORT \
    -t TEMP_DIR &

The `-fp` or `--file-server-port` set the port of file server. The file server is designed to upload and download
temporary files (like user defined `.class` files). Please make sure your `registry` server is running on the same
machine, for more information, please use `-h`  or `--help` option.

Then we need to start the `mapreduce-tasktracker` on every single node (or you can run mutiple instances in one node
for tests). here's the command

    $ dist/bin/mapreduce-tasktracker \
    -dh DFS_MASTER_REGISTRY_HOST \
    -dp DFS_MASTER_REGISTRY_PORT \
    -jh JOBTRACKER_REGISTRY_HOST \
    -jp JOBTRACKER_REGISTRY_PORT \
    -rp LOCAL_REGISTRY_PORT \
    -fp FILE_SERVER_PORT \
    -t TEMP_DIR &

The file server in `mapreduce-tasktracker` is designed to upload and download the intermediate files of mapper and
reducer tasks. Please make sure your `registry` server is running on the same machine, for more information, please use
`-h`  or `--help` option.

Now, the whole system is running, cheers!

####The word count example

The programming interface of SimpleMR is quite simple, you only need to extend the `AbstractMapReduce` class and
implement the `map` and `reduce` function, here is an example of how to implement a word count MapReduce program by
SimpleMR:

    import edu.cmu.courses.simplemr.mapreduce.AbstractMapReduce;
    import edu.cmu.courses.simplemr.mapreduce.OutputCollector;

    import java.util.Iterator;

    public class WordCount extends AbstractMapReduce {

        @Override
        public void map(String key, String value, OutputCollector collector) {
            String[] words = value.split("\\s+");
            for(String word : words){
                collector.collect(word, "1");
            }
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
            new WordCount().run(args);
        }
    }


Before we actually start the job, we need to load the data to DFS by using this command:

    $ dist/bin/dfs-load DATA_FILE

You can check the DFS after above command finished:

    $ dist/bin/dfs-ls

The `AbstractMapReduce` offers the command line argument parser, you can specifiy the MapReduce job options in the
command line, we also offered you a start script:

    $ dist/bin/examples-wordcount INPUT OUTPUT \
    -m MAPPER_NUMBER \
    -r REDUCER_NUMBER \
    -rh JOBTRACKER_REGISTRY_SERVER \
    -rp JOBTRACKER_REGISTRY_PORT \
    -n JOB_NAME

**NOTE:** the `INPUT` file name should *not* contains any file path, just name, since our DFS don't support file folder.
 To see more options, please use `-h` or `--help`.

After successfully submiting the job, you can use `mapreduce-jobs` to see the job status:

    $ dist/bin/mapreduce-jobs

    #0	wordcount	PENDING	(mapper task: 9 pending, 1 succeeded, 0 failed || reducer task: 5 pending, 0 succeeded, 0 failed)

Every job has 4 type of status: `INITIALIZING`, `PENDING`, `SUCCESS`, `FAILED`.


