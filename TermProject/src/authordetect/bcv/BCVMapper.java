package authordetect.bcv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu on 3/21/15.
 * Input Key: LongWritable ---> Offset in the file
 * Input Value: Text ---> "Title  Word TFIDF"
 * Output Key: Text ---> Title
 * Output Value: Text ---> "Word=TFIDF"
 */
public class BCVMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inVal = value.toString();
        String[] inSplit = inVal.split("\\t");
        String title = inSplit[0];
        String word = inSplit[1].split("\\|")[0];
        String tfidf = inSplit[1].split("\\|")[1];

        Text outKey = new Text(title);
        Text outVal = new Text(word + "=" + tfidf);
        context.write(outKey, outVal);
    }
}
