package authordetect.similarity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yqiu on 4/3/15.
 * Write out:
 * Output Key: Text ---> Book Title_Author
 * Output Value: DoubleWritable ---> Similarity
 */
public class SimilarityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double minSim = 0.0;
        for (Text val : values) {
            Double curSim = Double.parseDouble(val.toString());
            if (curSim < minSim) {
                minSim = curSim;
            }
        }
        context.write(key, new Text(minSim.toString()));

    }
}
