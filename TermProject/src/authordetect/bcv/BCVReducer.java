package authordetect.bcv;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 3/21/15.
 * Input Key: Text ---> Title
 * Input Value: Text ---> "Word=TFIDF"
 * Output Key: Text ---> Title
 * Output Value: Text ---> "Word1=TFIDF1,Word2=TFIDF2, ... "
 */
public class BCVReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String outStr = "";

        for (Text val : values) {
            String valStr = val.toString();
            outStr = outStr + "," + valStr;
        }

        Text outVal = new Text(outStr.substring(1));

        context.write(key, outVal);

    }
}
