import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetupMR extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(KMeansMR.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SetupMR(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Path centroids = new Path(output, "centroids");
        Path counts = new Path(output, "counts");

        Path plot = new Path(output, "plot");

        LOG.info("run prepossessing job");
        CountJob.run(getConf(), input, counts);

        LOG.info("run prepossessing job");
        PlotJob.runJob(getConf(), counts, new Path(output, "plot"));

        return 0;
    }
}
