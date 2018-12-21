package cs6240.hw2;

import com.google.common.collect.Iterables;
import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;
import java.util.TreeSet;

public class ReplicatedJoin extends BaseMR {
    public static final Logger LOG = LoggerFactory.getLogger(ReplicatedJoin.class);

    private static int max_accepted_id = Integer.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReplicatedJoin(), args);
    }

    @Override
    protected Iterable<Job> setupJobs(String[] args) throws Exception {
        Job job0 = createJob("linked", true);

        job0.addCacheFile(new Path(args[0]).toUri());
        job0.setJarByClass(ReplicatedJoin.class);
        job0.setMapperClass(LinkedMapper.class);
        job0.setReducerClass(LinkedReducer.class);
        job0.setInputFormatClass(TextInputFormat.class);

        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(NullWritable.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path(args[1], "job1"));

        if (args.length == 3) {
            max_accepted_id = Integer.parseInt(args[2]);
        }

        return Arrays.asList(job0);
    }


    public static class LinkedMapper extends Mapper<Object, Text, Text, NullWritable> {
        MultiValueMap<Integer, Integer> _follows = MultiValueMap.multiValueMap(new TreeMap<>(), TreeSet.class);

        private final Text _key = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                try {
                    reader.lines().forEach(line -> {
                        String[] uids = line.split(",");
                        if (uids.length != 2) return;

                        int from = Integer.parseInt(uids[0]);
                        int to = Integer.parseInt(uids[1]);
                        if (from == to || from > max_accepted_id || to > max_accepted_id) return;

                        _follows.put(from, to); //
                        _follows.put(-to, from); // incoming

                    });
                } finally {
                    reader.close();
                }
                LOG.info("read {} follows from cache", _follows.totalSize());
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] uids = value.toString().split(",");

            Integer first = Integer.parseInt(uids[0]);
            Integer second = Integer.parseInt(uids[1]);

            if (first > max_accepted_id || second > max_accepted_id)
                return;

            Collection<Integer> incoming = _follows.getCollection(-first);
            Collection<Integer> outgoning = _follows.getCollection(second);

            if (incoming == null || outgoning == null || incoming.isEmpty() || outgoning.isEmpty())
                return;

            for (int i : incoming) {
                if (outgoning.contains(i)) {
                    int[] triangle = new int[]{first, second, i};

                    Arrays.sort(triangle);
                    _key.set(Arrays.toString(triangle));

                    context.write(_key, NullWritable.get());
                }
            }
        }
    }


    public static class LinkedReducer extends Reducer<Text, NullWritable, Text, IntWritable> {
        //FollowPath s = new FollowPath();
        IntWritable s = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            s.set(Iterables.size(values));
//            ArrayList<IntWritable> aa = new ArrayList<>();
//            for (IntWritable a : values) {
//                aa.add(new IntWritable(a.get()));
//            }
//            s.set(aa.toArray(new IntWritable[0]));

            context.write(key, s);
        }
    }
}
