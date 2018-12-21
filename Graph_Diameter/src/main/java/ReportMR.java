import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReportMR {
    private static final Logger LOG = LoggerFactory.getLogger(ReportMR.class);

    private final static String _LONGEST_PAIR_KEY = "&&";

    public static class DistancesCombiner extends Mapper<Object, Text, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable outVal = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("\\s+", 2);

            if (nodes.length != 2) {
                LOG.warn("skip bad input: {}", value.toString());
                return;
            }

            JoinPath jp = JoinPath.parse(nodes[1]);

            outKey.set(String.join(",", jp.paths.get(0), nodes[0]));
            outVal.set(jp.cost);

            context.write(outKey, outVal); // output all pair result

            //outKey.set(_LONGEST_PAIR_KEY);
            //context.write(outKey, outVal);
        }
    }

    public static class ShortestDistances extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int min = Integer.MAX_VALUE;

            for (IntWritable t : values) {
                if (t.get() < min) {
                    min = t.get();
                }
            }

            context.write(key, new IntWritable(min));
        }
    }


    public static boolean runJob(Configuration conf, Path sample,  Path result, Path output, int numIterations) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, DiameterMR.class.getSimpleName());

        job.setJarByClass(DiameterMR.class);

        job.setMapperClass(DistancesCombiner.class);
        job.setReducerClass(ShortestDistances.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, sample, TextInputFormat.class, DistancesCombiner.class);
        for (int i = 1; i <= numIterations; i++) {
            MultipleInputs.addInputPath(job, result.suffix(Integer.toString(i)), TextInputFormat.class, DistancesCombiner.class);
        }

        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);

    }
}
