package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JobTrackerScheduler implements Runnable{
    private static Logger LOG = LoggerFactory.getLogger(JobTrackerScheduler.class);

    private JobTracker jobTracker;
    private ExecutorService pool;

    public JobTrackerScheduler(JobTracker jobTracker, int poolSize){
        this.jobTracker = jobTracker;
        if(poolSize <= 0){
            this.pool = Executors.newFixedThreadPool(Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
        } else {
            this.pool = Executors.newFixedThreadPool(poolSize);
        }
    }

    @Override
    public void run() {
        while(true){
            try {
                MapperTask task = jobTracker.takeMapperTask();
                task.setStatus(TaskStatus.PENDING);
                JobTrackerDispatcher dispatcher = new JobTrackerDispatcher(jobTracker, task);
                pool.execute(dispatcher);
            } catch (InterruptedException e) {
                LOG.error("Job scheduler is interrupted!", e);
                System.exit(-1);
            }
        }
    }
}
