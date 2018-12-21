import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AdjacencyListMR {
    private static final Logger LOG = LoggerFactory.getLogger(AdjacencyListMR.class);


    public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
        private final Text source = new Text();
        private final Text dest = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] nodes = line.split(",");
            source.set(nodes[0]);
            dest.set(nodes[1]);
            context.write(source, dest);
        }
    }

    public static class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        public void reduce(Text node, Iterable<Text> neighbours,
                           Context context) throws IOException, InterruptedException {

            StringBuffer adjacencyList = new StringBuffer().append("c:");  // tag

            for (Text neighbour : neighbours) {
                adjacencyList.append(neighbour);
                adjacencyList.append(",");
            }
            String resultString =
                    adjacencyList.length() > 2 ?
                            adjacencyList.deleteCharAt(adjacencyList.length() - 1).toString() : "";
            result.set(resultString);
            context.write(node, result);
        }
    }


    public static boolean runJob(Path input, Path output, Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, AdjacencyListMR.class.getSimpleName());

        job.setJarByClass(AdjacencyListMR.class);
        job.setMapperClass(EdgeMapper.class);
        job.setReducerClass(AdjacencyListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }
}
