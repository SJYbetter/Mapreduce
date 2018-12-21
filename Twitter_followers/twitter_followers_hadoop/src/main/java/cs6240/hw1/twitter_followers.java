package cs6240.hw1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;

public class twitter_followers extends Configured implements Tool {
//    private static final Logger logger = LogManager.getLogger(twitter_followers.class);

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        private final Text word = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            //logger.info("map  key: `" + key.toString() + "` value: `" + value.toString() + "`");
            //System.out.printf("map key: %s value: %s\n", key.toString(), value.toString());

            final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            if (itr.hasMoreTokens()){
                word.set(itr.nextToken());

                // map to (user_id, 0) if nodes.csv or (user_id, 1) if edges.csv
                context.write(word, itr.hasMoreTokens() ?  one : zero);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "twitter_followers");
        job.setJarByClass(twitter_followers.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

        // debug options
//        jobConf.set(MRJobConfig.JOB_RUNNING_MAP_LIMIT, "1");
//        jobConf.set("mapreduce.map.java.opts", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
//        jobConf.set("mapreduce.reduce.java.opts", "--add-modules=ALL-SYSTEM");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<nodes.csv> <edges.csv> <output-dir>");
        }

        ToolRunner.run(new twitter_followers(), args);
    }
}
