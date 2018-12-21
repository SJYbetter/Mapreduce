import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// load JoinPath to (key, value) key => to, value => cost, from paths
public class ReadMapper extends Mapper<Object, Text, Text, Text> {
    final Text outKey = new Text();
    final Text outVal = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] nodes = value.toString().split("\\s+", 2);

        outKey.set(nodes[0]);
        outVal.set(nodes[1]);

        context.write(outKey, outVal);
    }
}
