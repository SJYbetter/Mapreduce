import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class SampleMR {

    public static class SampleMapper extends
            Mapper<Object, Text, NullWritable, Text> {

        private Random rands = new Random();
        private double k;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            k = context.getConfiguration().getFloat("filter_percentage", 0.3f);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            if (rands.nextDouble() < k) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    // create sample job
    public static boolean runJob(Configuration conf, double k, String input_file, Path output) throws IOException, ClassNotFoundException, InterruptedException {

        conf.setDouble("filter_percentage", k);

        Job job = Job.getInstance(conf, SampleMR.class.getSimpleName());
        job.setJarByClass(SampleMR.class);

        job.setMapperClass(SampleMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0); // Set number of reducers to zero
        FileInputFormat.addInputPath(job, new Path(input_file));
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(false);
    }
}
