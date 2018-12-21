import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansMR extends Configured implements Tool {
    public static final String K = "__K_K_";
    public static final String TOTAL_RECORDS = "__total_records_";
    private static final Logger LOG = LoggerFactory.getLogger(KMeansMR.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new KMeansMR(), args);
    }


//    public  void processCmdline(){
//        OptionGroup group = new OptionGroup();
//        group.addOption()
//        Options options = new Options();
//
//    }

    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int k = Integer.parseInt(args[2]);

        boolean hasCentroid = args.length >= 4 && !"random".equals(args[3]);

        getConf().setInt(K, k);

        Path counts = new Path(input, "counts");
        Path plot = new Path(input, "plot");

        Path centroids = null;

        if (hasCentroid) {
            centroids = new Path(args[3]);
        } else {
            LOG.info("run samples centroids job");
            centroids = new Path(output, "centroids");
            SampleJob.runJob(getConf(), plot, centroids);
        }

        long lastSSE = 0;

        for (int i = 0; i < 10; i++) {
            Path kOut = new Path(output, "k_means_" + i);
            long total_sse = KMeansJob.runJob(getConf(), counts, centroids, kOut, i);

            LOG.info("iteration: {} total error: {}", i, total_sse);

            if (lastSSE == total_sse) break;
            centroids = kOut;
        }

        return 0;
    }
}
