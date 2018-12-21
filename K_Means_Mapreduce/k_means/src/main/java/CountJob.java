import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountJob {

    public static class InputRead extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable _key = new IntWritable(0);
        private IntWritable _val = new IntWritable(0);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            _key.set(Integer.parseInt(parts[0]));
            _val.set(Integer.parseInt(parts[1]));

            context.write(_key, _val);
        }
    }

    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable _val = new IntWritable(0);
        private Counter _counter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            _counter = context.getCounter(TotalRecord.RAW_RECORD_COUNTER);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int y = 0;
            for (IntWritable x : values) y++;

            _val.set(y);
            context.write(key, _val);

            _counter.increment(1);
        }
    }

    public static void run(Configuration configuration, Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(configuration, CountJob.class.getSimpleName());
        job.setJarByClass(CountJob.class);

        job.setMapperClass(InputRead.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (!job.waitForCompletion(true))
            throw new IllegalStateException("Job " + CountJob.class.getSimpleName() + " failed");

        Counters changesCounter = job.getCounters();
        long total_counts = changesCounter.findCounter(TotalRecord.RAW_RECORD_COUNTER).getValue();
        configuration.setLong(KMeansMR.TOTAL_RECORDS, total_counts);
    }

    public static enum TotalRecord {

        RAW_RECORD_COUNTER
    }
}
