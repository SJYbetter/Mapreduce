import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class S3TestJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "twasink");

        FileSystem fs = new Path(strings[0]).getFileSystem(getConf());

        // FileSystem fs = FileSystem.get(getConf());

        FileStatus status = fs.getFileStatus(new Path(strings[0]));

        if (status.isDirectory())
            throw new IllegalStateException("testdata");

        return 0;
    }

    public static class M extends Mapper<Object, Text, NullWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.getCacheFiles();

            super.setup(context);
        }
    }


    public static void main(String[] args) throws Exception {

        ToolRunner.run(new S3TestJob(), args);

    }
}
