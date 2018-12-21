import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class DiameterMR {
    private static final Logger LOG = LoggerFactory.getLogger(DiameterMR.class);

    static enum MorePossibility {
        numUpdated
    }

//    public static class PathOrEdgeMapper extends Mapper<Object, Text, Text, Text> {
//
//        @Override
//        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
//        }
//    }
//
//
//    public static class AdjacencyListMapper extends Mapper<Object, Text, Text, Text> {
//        final Text outKey = new Text();
//        final Text outVal = new Text();
//
//
//        @Override
//        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//
//            LOG.info("xxxx mapper: {} {}", key, value);
//
//            String[] nodes = value.toString().split(",");
//            if (nodes.length != 2) {
//                LOG.warn("skip bad input `{}`", value.toString());
//            }
//
//            outKey.set(nodes[1]);
//            outVal.set(nodes[0]);
//
//            context.write(outKey, outVal);
//        }
//    }


    public static class DiameterReducer extends Reducer<Text, Text, Text, Text> {
        private Counter numUpdated;

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numUpdated = context.getCounter(MorePossibility.numUpdated);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] adjacencyList = null;

            List<JoinPath> paths = new LinkedList<JoinPath>();

            for (Text s : values) {
                String record = s.toString();

                if (record.startsWith("c:")) {
                    adjacencyList = record.substring(2).split(",");
                } else if (record.startsWith("p:")) {
                    paths.add(JoinPath.parse(record.substring(2)));
                } else {
                    LOG.warn("unknown value: {}", s);
                    return;
                }
            }

            if (adjacencyList == null)
                return;

            for (JoinPath jp : paths) {
                jp.push(key.toString());

                for (String next : adjacencyList) {
                    if (jp.paths.contains(next))   // loopback
                        continue;

                    outKey.set(next);
                    outVal.set(jp.toString());

                    context.write(outKey, outVal);

                    numUpdated.increment(1);
                }
            }
        }
    }


    public static boolean runJob(Configuration conf, Path edges, Path adjacency, Path output) throws Exception {
        long numUpdated = 1;
        int numIterations = 1;

//        conf.set(MRJobConfig.JOB_RUNNING_REDUCE_LIMIT,"1");
//        conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");

//        conf.set(MRJobConfig.JOB_RUNNING_MAP_LIMIT, "1");
//        conf.set("mapreduce.map.java.opts", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");


        Path input = edges, tmp = null;

        while (numUpdated > 0) {
            Job job = Job.getInstance(conf, DiameterMR.class.getSimpleName());

            job.setJarByClass(DiameterMR.class);

            job.setMapperClass(ReadMapper.class);
            job.setReducerClass(DiameterReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            MultipleInputs.addInputPath(job, input, TextInputFormat.class, ReadMapper.class);
            MultipleInputs.addInputPath(job, adjacency, TextInputFormat.class, ReadMapper.class);

//        FileInputFormat.addInputPath(job, edges);
//        FileInputFormat.addInputPath(job, adjacency);

            FileOutputFormat.setOutputPath(job, tmp = output.suffix(Integer.toString(numIterations)));

            job.waitForCompletion(true);

            Counters changesCounter = job.getCounters();
            numUpdated = changesCounter.findCounter(MorePossibility.numUpdated).getValue();

            input = tmp;
            numIterations++;
        }

        ReportMR.runJob(conf, edges, output, new Path(output.getParent(), "report"), --numIterations);


        return true;
    }
}
