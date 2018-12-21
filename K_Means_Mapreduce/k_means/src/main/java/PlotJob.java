import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.DefaultXYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.Map;

public class PlotJob {
    private static Logger LOG = LoggerFactory.getLogger(KMeansJob.class);


    // input:  fp  userId  followerCount   output: followerCount  userId
    public static class M extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable _key = new IntWritable();
        private IntWritable _val = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] p = value.toString().split("\\s+");
            if (p.length != 2) {
                LOG.warn("mapper: bad input {}", value.toString());
            } else {
                _key.set(Integer.parseInt(p[1]));
                _val.set(Integer.parseInt(p[0]));
                context.write(_key, _val);
            }
        }
    }

    // input: followerCount userId  output: followerCount userCount
    public static class R extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private LinkedList<Map.Entry<Integer, Integer>> _points = new LinkedList<>();

        private IntWritable _val = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            final int[] c = {0};
            values.forEach(i -> c[0]++);

            _val.set(c[0]);
            context.write(key, _val);

            _points.add(new AbstractMap.SimpleImmutableEntry<Integer, Integer>(key.get(), c[0]));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            double[] x = new double[_points.size()];
            double[] y = new double[_points.size()];

            for (int i = 0; _points.size() > 0; i++) {
                Map.Entry<Integer, Integer> p = _points.removeFirst();
                x[i] = p.getKey();
                y[i] = p.getValue();
            }

            DefaultXYDataset ds = new DefaultXYDataset();
            ds.addSeries("default", new double[][]{x, y});

            JFreeChart chart = ChartFactory.createScatterPlot("points", "follower count", "user count", ds,
                    PlotOrientation.VERTICAL, true, true, false);
            XYPlot plot = chart.getXYPlot();
            plot.getRenderer().setSeriesPaint(0, Color.BLUE);

            BufferedImage buf_img = chart.createBufferedImage(1600, 1024);

            Path output = new Path(context.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"), "plot.png");

            FileSystem fs = output.getFileSystem(context.getConfiguration());

            try (OutputStream stream = fs.create(output)) {
                ChartUtilities.writeBufferedImageAsPNG(stream, buf_img);
            }
        }
    }

    public static void runJob(Configuration config, Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config);

        job.setJobName(PlotJob.class.getSimpleName());

        job.setJarByClass(PlotJob.class);

        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (!job.waitForCompletion(true))
            throw new IllegalStateException("plot job failed");
    }
}
