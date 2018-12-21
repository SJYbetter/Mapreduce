import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

public class KMeansJob {
    public static final String CENTROIDS_OUTPUT_PREFIX = "centroids";

    private static Logger LOG = LoggerFactory.getLogger(KMeansJob.class);

    public enum SSE {TOTAL}

    public static class M extends Mapper<Object, Text, IntWritable, Text> {
        TreeMap<Integer, Double> centroids = new TreeMap<>();
        IntWritable _clusterKey = new IntWritable();
        Text _node = new Text();
        Counter _sse = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            _sse = context.getCounter(SSE.TOTAL);

            LOG.info("total sse init value {}", _sse.getValue());

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IllegalArgumentException("centroids not found");
            }

            for (URI file : cacheFiles) {
                Path path = new Path(file);

                FileSystem fs = path.getFileSystem(context.getConfiguration());

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    reader.lines().forEach(line -> {
                        String[] p = line.trim().split("[,\\s]+");
                        if (p.length != 2) {
                            LOG.warn("centroids: bad input {}", line);
                        } else {
                            centroids.put(Integer.parseInt(p[0]), Double.parseDouble(p[1]));
                        }
                    });
                }
            }
        }
        // map funtion
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] p = value.toString().split("[,\\s]+");
            if (p.length != 2) {
                LOG.warn("mapper: bad input: {}", value.toString());
                return;
            }

            Integer myLength = Integer.parseInt(p[1]);
            Double minDistance = Double.MAX_VALUE;

            Map.Entry<Integer, Double> minEntry = null;

            for (Map.Entry<Integer, Double> one : centroids.entrySet()) {
                //I use the abs rather then square due to overflow
                Double distance = Math.abs(myLength - one.getValue());
                if (distance < minDistance) {
                    minEntry = one;
                    minDistance = distance;
                }
            }
            // update the sse
            _sse.increment(Math.round(minDistance));

            _clusterKey.set(minEntry.getKey());
            _node.set(value);

            context.write(_clusterKey, _node);
        }
    }

    public static class R extends Reducer<IntWritable, Text, IntWritable, Text> {
        DoubleWritable _center = new DoubleWritable();
        MultipleOutputs s;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            s = new MultipleOutputs(context);
        }


        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int total = 0, count = 0;

            TreeMap<Integer, Integer> user_followers = new TreeMap<>();
            for (Text t : values) {
                String[] p = t.toString().split("[,\\s]+");
                if (p.length != 2) {
                    LOG.warn("reducer bad input: {}", t.toString());
                    return;
                }

                int followerCount = Integer.parseInt(p[1]);
                int userId = Integer.parseInt(p[0]);
                user_followers.put(userId, followerCount);

                count++;
                total += followerCount;
                context.write(key, t);
            }

            double center = (total * 1.0) / count;

//            double minDistance = Double.MAX_VALUE;
//            Map.Entry<Integer, Integer> nearest = user_followers.firstEntry();
//            for (Map.Entry<Integer, Integer> entry : user_followers.entrySet()) {
//                double distance = Math.abs(entry.getValue() - center);
//                if (distance < minDistance) {
//                    minDistance = distance;
//                    nearest = entry;
//                }
//            }
//            s.write(nearest.getKey(), nearest.getValue(), CENTROIDS_OUTPUT_PREFIX);
            s.write(key, center, CENTROIDS_OUTPUT_PREFIX);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            s.close();
        }
    }


    /* return total sse */
    public static long runJob(Configuration config, Path counts, Path centroids, Path output, long seq)
            throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(config, KMeansJob.class.getSimpleName());

        // job.getConfiguration().set(MRJobConfig.MAP_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");

        job.setJarByClass(KMeansJob.class);
        job.setMapperClass(KMeansJob.M.class);
        job.setReducerClass(KMeansJob.R.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, counts);
        FileOutputFormat.setOutputPath(job, output);

        // add centroids to cache
        FileSystem fs = centroids.getFileSystem(config);

        if (fs.getFileStatus(centroids).isFile()) {
            job.addCacheFile(centroids.toUri());
        } else {
            FileStatus[] fileList = fs.listStatus(centroids, path -> seq == 0 ? path.getName().startsWith("part-") : path.getName().startsWith(CENTROIDS_OUTPUT_PREFIX));
            for (FileStatus file : fileList) {
                job.addCacheFile(file.getPath().toUri());
            }
        }

        if (!job.waitForCompletion(true))
            throw new IllegalStateException("run k-means job failed");

        Counter sse = job.getCounters().findCounter(SSE.TOTAL);
        return sse.getValue();
    }
}
