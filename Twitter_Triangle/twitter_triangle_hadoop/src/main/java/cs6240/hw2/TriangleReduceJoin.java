package cs6240.hw2;

import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class TriangleReduceJoin extends BaseMR {

    public static final Logger LOG = LoggerFactory.getLogger(TriangleReduceJoin.class);

    public static void main(final String[] args) throws Exception {
//        if (args.length != 2) {
//            throw new Error("Two arguments required:\n<edges.csv> <output-dir>");
//        }
        ToolRunner.run(new TriangleReduceJoin(), args);
    }

    @Override
    protected Iterable<Job> setupJobs(String[] args) throws Exception {
        Job job0 = createJob("linked", false);

        job0.setInputFormatClass(TextInputFormat.class);
        job0.setMapperClass(LinkedMapper.class);
        job0.setCombinerClass(LinedReducer.class);
        job0.setReducerClass(LinedReducer.class);
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(Text.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        FileInputFormat.setMinInputSplitSize(job0, 1);// 设定文件分片，这样才能让多个mapper和reducer实际用起来
        FileInputFormat.setMaxInputSplitSize(job0, 1048576 * 3);
        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path(args[1], "job1"));


        Job job1 = createJob("triangle", false);
        job1.setMapperClass(TriangleMapper.class);
        job1.setPartitionerClass(TrianglePartitioner.class);
        job1.setReducerClass(TriangleReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job1, new Path(args[1], "job1"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1], "job2"));


        Job job2 = createJob("report", false);
        job2.setMapperClass(ReportMapper.class);
        job2.setReducerClass(ReportReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1], "job2"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1], "job3"));

        return Arrays.asList(job0, job1, job2);
    }

    public static class LinkedMapper extends Mapper<Object, Text, Text, Text> {

        private final Text from = new Text();
        private final Text to = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            if (itr.hasMoreTokens()) {
                from.set(itr.nextToken());
                to.set(itr.nextToken());

                context.write(from, to);
                context.write(to, from);
            }
        }
    }

    public static class LinedReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            result.set(String.join(",", Iterables.transform(values, v -> v.toString())));

            context.write(key, result);
        }
    }

    public static class TriangleMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text _key = new Text();
        private final Text _val = new Text();
        //private final ArrayWritable _row = new ArrayWritable(IntWritable.class);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString());
            scanner.useDelimiter("[,\t\r\n]");

            int from = scanner.nextInt();
            TreeSet<Integer> peers = new TreeSet<>();
            while (scanner.hasNext()) {
                peers.add(scanner.nextInt());
            }

            String from_txt = Integer.toString(from);

            for (int peer : peers) {
                _key.set(String.join(":", from_txt, Integer.toString(peer)));

                Iterable<String> left_peers = Iterables.transform(peers.tailSet(peer), v -> Integer.toString(v));

                //_row.set(Iterables.toArray(left_peers, Writable.class));

                _val.set(String.join(",", left_peers));
                context.write(_key, _val);
            }
        }
    }

    public static class TrianglePartitioner extends HashPartitioner<Text, Text> {

        @Override
        public int getPartition(Text text, Text text2, int i) {
            return super.getPartition(new Text(text.toString().split(":")[0]), text2, i);
        }
    }

    public static class TriangleReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //String line = String.join(" *** ", Iterables.transform(values, i -> i.toString()));

            //LOG.info("ssssss {} {} {}", context.getJobID(), key, line);

            for (Text val : values) {
                for (String next : val.toString().split(",")) {
                    context.write(new Text(key.toString() + ":" + next), new IntWritable(1));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class ReportMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String txtKey = value.toString().split("\t")[0];
            String[] keys = txtKey.split(":");

            int[] x = Arrays.stream(keys).mapToInt(o -> Integer.parseInt(o)).sorted().toArray();

            String[] y = Arrays.stream(x).mapToObj(String::valueOf).toArray(String[]::new);

            context.write(new Text(String.join(":", y)), one);
        }
    }

    public static class ReportReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            if (sum >= 3) {
                result.set(sum);
                context.write(key, result);
            }
//            super.reduce(key, values, context);
        }
    }
}
