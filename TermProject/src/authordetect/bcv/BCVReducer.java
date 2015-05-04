package authordetect.bcv;

import authordetect.AuthorDetection;
import authordetect.structure.WordTFIDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by Qiu on 3/21/15.
 * Input Key: Text ---> Title
 * Input Value: Text ---> "Word=TFIDF"
 * Output Key: Text ---> Title
 * Output Value: Text ---> "Word1=TFIDF1,Word2=TFIDF2, ... "
 */
public class BCVReducer extends Reducer<Text, Text, Text, Text> {

    private ArrayList<WordTFIDF> wordTFIDFs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        wordTFIDFs = new ArrayList<WordTFIDF>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String outStr = "";

        for (Text val : values) {
            String valStr = val.toString();
            String word = valStr.split("=")[0];
            Double tfidf = Double.parseDouble(valStr.split("=")[1]);
            WordTFIDF element = new WordTFIDF(word, tfidf);
            wordTFIDFs.add(element);
        }

        Collections.sort(wordTFIDFs);
        int idx = wordTFIDFs.size();
        for (int i = 0; i < AuthorDetection.TOP_TFIDF; i++) {
            WordTFIDF wordTFIDF = wordTFIDFs.get(--idx);
            String outvalStr = wordTFIDF.getWord() + "=" + wordTFIDF.getTfidf();
            Text outVal = new Text(outvalStr);
            context.write(key, outVal);
        }

    }
}
