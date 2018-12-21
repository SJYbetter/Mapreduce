import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class SampleJob {
    private static Logger LOG = LoggerFactory.getLogger(SampleJob.class);

    public static class SampleMapper extends Mapper<Object, Text, NullWritable, Text> {
        private double _percent = 0;
        private Random _rand = new Random();

        private IntWritable _const_key = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            int k = context.getConfiguration().getInt(KMeansMR.K, -1);
            if (k <= 0)
                throw new IllegalArgumentException("K must be great then 0");

            long total_records = context.getConfiguration().getLong(KMeansMR.TOTAL_RECORDS, 3351); //8768419
            if (total_records <= 0)
                throw new IllegalArgumentException("TOTAL_RECORDS must be great then 0");

            _percent = k * 6.0 / total_records;

            LOG.info("total {} records, sample {}%", total_records, _percent);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (_rand.nextDouble() <= _percent) {
                LOG.info("{} -> {}", key, value);
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static class SampleReducer extends Reducer<NullWritable, Text, IntWritable, IntWritable> {
        private int _k = -1;
        private IntWritable _seq = new IntWritable(0);
        private IntWritable _point = new IntWritable(0);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            _k = context.getConfiguration().getInt(KMeansMR.K, -1);
            if (_k <= 0)
                throw new IllegalArgumentException("K must be great then 0");
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> all_samples = new ArrayList<>();
            values.forEach(v -> {
                all_samples.add(v.toString());
                LOG.info("input {}", v.toString());
            });

            LOG.info("got {} records, sample {} records", all_samples.size(), _k);

            Text text = new Text();
            Random rand = new Random();
            while (_k > 0) {
                int index = rand.nextInt(all_samples.size());
                if (index < 0 || index >= all_samples.size())
                    continue;
                String v = all_samples.get(index);
                if (v == null) continue;

                all_samples.set(index, null);
                // text.set(v);

                _seq.set(0 - _k);
                LOG.info("read sample data {}", v);
                _point.set(Integer.parseInt(v.toString().split("[,\\s]+")[0]));

                context.write(_seq, _point);
                _k--;

                LOG.info("sample record: {}, index {} ", v.toString(), index, all_samples.size());
            }
        }
    }

    public static void runJob(Configuration config, Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, SampleJob.class.getSimpleName());

        job.setJarByClass(SampleJob.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        // config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");
        // config.set(MRJobConfig.MAP_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // job.getConfiguration().set("mapreduce.output.basename", KMeansJob.CENTROIDS_OUTPUT_PREFIX);

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
