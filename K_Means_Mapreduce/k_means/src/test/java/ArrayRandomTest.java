import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class ArrayRandomTest {

    @Test
    public void random_item() {
        int[] numbers = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        Integer[] all_samples = Iterables.toArray(Arrays.asList(1,2,3,4,5,6),Integer.class);

        Random rand = new Random();

        int _k = 10;
        while (_k > 0) {

            int index = rand.nextInt(numbers.length);
            if (index < 0 || index >= numbers.length)
                continue;

            int v = numbers[index];
            if (v == 0) continue;

            numbers[index] = 0;
            System.out.println(v);
            _k--;
        }

    }

    @Test
    public  void testP(){
        String x = "6610101        1448";

        String[] p0=  x.split("\\s+");

        System.out.println(p0);
    }
}
