import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Boot extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new Boot(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        String output = args[2];

        Path sample_output = new Path(output, "samples");
        Path normalize_output = new Path(output, "normal");
        Path adjacency_output = new Path(output, "adjacency");

        Path result_output = new Path(output, "result");

        if (!SampleMR.runJob(conf, Double.parseDouble(args[1]), args[0], sample_output))
            return -1;

        if (!JoinPathMR.runJob(sample_output, normalize_output, conf))
            return -2;

        if (!AdjacencyListMR.runJob(sample_output, adjacency_output, conf))
            return -3;

        if (!DiameterMR.runJob(this.getConf(), normalize_output, adjacency_output, result_output))
            return -4;

        return 0;
    }
}
