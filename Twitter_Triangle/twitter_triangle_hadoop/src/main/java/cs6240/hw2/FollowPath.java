package cs6240.hw2;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class FollowPath extends ArrayWritable implements WritableComparable<FollowPath> {

    public FollowPath() {
        super(IntWritable.class);
    }


    @Override
    public int compareTo(FollowPath o) {
        IntWritable[] my = get();
        IntWritable[] other = o.get();

        for (int i = 0; i < my.length; i++) {
            if (other.length <= i) {
                return 1;
            }
            int result = my[i].compareTo(other[i]);
            if (result != 0)
                return result;
        }

        return other.length > my.length ? -1 : 0;
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }

    @Override
    public String toString() {
        return String.join(",", this.toStrings());
    }

}
