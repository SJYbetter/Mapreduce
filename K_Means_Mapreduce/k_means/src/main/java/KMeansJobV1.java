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
import java.util.TreeSet;

public class KMeansJobV1 {

    private static Logger LOG = LoggerFactory.getLogger(KMeansJobV1.class);


    // output:  cluster Id,  followerCounter
    public static class M extends Mapper<Object, Text, IntWritable, IntWritable> {
        TreeMap<Integer, Double> centroids = new TreeMap<>();
        IntWritable _clusterKey = new IntWritable();
        Text _node = new Text();
        IntWritable _followerCount = new IntWritable();
        Counter _sse = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            _sse = context.getCounter(KMeansJob.SSE.TOTAL);

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
                            int id = Integer.parseInt(p[0]);
                            double val = Double.parseDouble(p[1]);
                            if (id > 0)
                                throw new IllegalArgumentException("centroid id must be leas then zero");
                            centroids.put(id, val);
                        }
                    });
                }
            }
        }

        @Override
        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] p = line.toString().split("[,\\s]+");
            if (p.length != 2) {
                LOG.warn("mapper: bad input: {}", line.toString());
                return;
            }

            Integer myCount = Integer.parseInt(p[0]);

            Double minDistance = Double.MAX_VALUE;
            Map.Entry<Integer, Double> minEntry = null;

            for (Map.Entry<Integer, Double> one : centroids.entrySet()) {
                Double distance = Math.abs(myCount - one.getValue());
                if (distance < minDistance) {
                    minEntry = one;
                    minDistance = distance;
                }
            }
            _sse.increment(Math.round(minDistance));

            _clusterKey.set(minEntry.getKey());
            // _node.set(line);

            _followerCount.set(myCount);

            context.write(_clusterKey, _followerCount);
        }
    }

    public static class R extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        DoubleWritable _center = new DoubleWritable();
        MultipleOutputs s;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            s = new MultipleOutputs(context);
        }


        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0, count = 0;

            TreeSet<Double> user_followers = new TreeSet<>();
            for (IntWritable v : values) {
                count++;
                total += v.get();
                user_followers.add((double) v.get());
                //context.write(key, t);
            }

            double center = (total * 1.0) / count;

            double floor = user_followers.floor(center);
            double higher = user_followers.higher(center);

            if (Math.abs(floor - center) < Math.abs(higher - center)) {
                s.write(key, floor, KMeansJob.CENTROIDS_OUTPUT_PREFIX);
            } else {
                s.write(key, higher, KMeansJob.CENTROIDS_OUTPUT_PREFIX);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            s.close();
        }
    }


    /* return total sse */
    public static long runJob(Configuration config, Path plot, Path centroids, Path output, long seq)
            throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(config, KMeansJobV1.class.getSimpleName());

        // job.getConfiguration().set(MRJobConfig.MAP_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");

        job.setJarByClass(KMeansJobV1.class);
        job.setMapperClass(KMeansJobV1.M.class);
        job.setReducerClass(KMeansJobV1.R.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, plot);
        FileOutputFormat.setOutputPath(job, output);

        // add centroids to cache
        FileSystem fs = centroids.getFileSystem(config);

        if (fs.getFileStatus(centroids).isFile()) {
            job.addCacheFile(centroids.toUri());
        } else {
            FileStatus[] fileList = fs.listStatus(centroids, path -> seq == 0 ? path.getName().startsWith("part-")
                    : path.getName().startsWith(KMeansJob.CENTROIDS_OUTPUT_PREFIX));
            for (FileStatus file : fileList) {
                job.addCacheFile(file.getPath().toUri());
            }
        }

        if (!job.waitForCompletion(true))
            throw new IllegalStateException("run k-means job failed");

        Counter sse = job.getCounters().findCounter(KMeansJob.SSE.TOTAL);
        return sse.getValue();
    }
}
