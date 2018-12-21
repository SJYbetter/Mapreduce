import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class JoinPathMR {
    private static final Logger LOG = LoggerFactory.getLogger(DiameterMR.class);


    // map format "from,to" to "to\t1 cost paths"
    public static class NormalizeMapper extends Mapper<Object, Text, Text, Text> {
        final Text outKey = new Text();
        final Text outVal = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] nodes = value.toString().split(",");
            if (nodes.length != 2) {
                LOG.warn("skip bad input `{}`", value.toString());
            }

            outKey.set(nodes[1]);
            outVal.set(new JoinPath().push(nodes[0]).toString());

            context.write(outKey, outVal);
        }
    }


    public static boolean runJob(Path samples, Path output, Configuration conf)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, JoinPathMR.class.getSimpleName());

        job.setJarByClass(JoinPathMR.class);

        job.setMapperClass(NormalizeMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, samples);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }
}
