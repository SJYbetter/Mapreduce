package cs6240.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

abstract class BaseMR extends Configured implements Tool {


    @Override
    public int run(final String[] args) throws Exception {
        Iterable<Job> all_jobs = setupJobs(args);
        for (Job job : all_jobs) {
            if (!job.waitForCompletion(true))
                return 0;
        }
        return 1;
    }

    protected abstract Iterable<Job> setupJobs(String[] args) throws Exception;


    protected Job createJob(String name, boolean enableDebug) throws IOException {
        Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + name);
        job.setJarByClass(this.getClass());

        final Configuration jobConf = job.getConfiguration();
        job.setMaxMapAttempts(1);
        job.setMaxReduceAttempts(1);
        job.setNumReduceTasks(1);

        jobConf.set(MRJobConfig.JOB_RUNNING_MAP_LIMIT, "1");
        jobConf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1g");

        //jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        if (enableDebug) {

            //jobConf.set(MRJobConfig.MAP_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");
            jobConf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");
        }


        return job;
    }
}