package prefilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by Qiu on 04/29/2015.
 */
public class DataFilterReducer extends Reducer<Text, Text, Text, Text> {

    private final int AUTHOR_BOOKS_MIN = 10;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Text> set = new HashSet<>();
        for (Text filename : values) {
            set.add(filename);
        }

        if (set.size() >= AUTHOR_BOOKS_MIN) {
            for (Text filename : set) {
                context.write(new NullWritable(), filename);
            }
        }
    }
}
