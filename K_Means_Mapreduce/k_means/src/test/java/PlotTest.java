import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.DefaultXYDataset;
import org.junit.Test;
import scala.Tuple2;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

public class PlotTest {

    @Test
    public void plot() throws IOException, InterruptedException {
        LinkedList<Tuple2<Integer, Integer>> _points = new LinkedList<>();
        _points.add(new Tuple2<>(1, 2));
        _points.add(new Tuple2<>(3, 4));
        _points.add(new Tuple2<>(5, 6));
        _points.add(new Tuple2<>(7, 8));
        _points.add(new Tuple2<>(9, 10));

        double[] x = new double[_points.size()];
        double[] y = new double[_points.size()];

        for (int i = 0; _points.size() > 0; i++) {
            Tuple2<Integer, Integer> p = _points.removeFirst();
            x[i] = p._1();
            y[i] = p._2();
        }

        DefaultXYDataset ds = new DefaultXYDataset();
        ds.addSeries("default", new double[][]{x, y});

        JFreeChart chart = ChartFactory.createScatterPlot("points", "follower count", "user count", ds,
                PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = chart.getXYPlot();
        plot.getRenderer().setSeriesPaint(0, Color.BLUE);

        BufferedImage buf_img = chart.createBufferedImage(1600, 1024);


        try (OutputStream stream = new ByteArrayOutputStream()) {
            ChartUtilities.writeBufferedImageAsPNG(stream, buf_img);
        }

        ChartFrame frame = new ChartFrame("Test", chart);
        frame.pack();
        frame.setVisible(true);

        Thread.sleep(60 * 1000);
    }
}
