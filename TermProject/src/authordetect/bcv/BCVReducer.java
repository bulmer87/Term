package authordetect.bcv;

import authordetect.AuthorDetection;
import authordetect.structure.WordTFIDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

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
    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<WordTFIDF> wordTFIDFs = new ArrayList<WordTFIDF>();

        String eucOutStr = "";

        for (Text val : values) {
            // for calculating cosine similarity
            String valStr = val.toString();
            String word = valStr.split("=")[0];
            Double tfidf = Double.parseDouble(valStr.split("=")[1]);
            WordTFIDF element = new WordTFIDF(word, tfidf);
            wordTFIDFs.add(element);
            // for calculating euclidean distance
            eucOutStr = eucOutStr + "," + valStr;
        }

        //write out all tfidf
        mos.write("euclidean", key, new Text(eucOutStr.substring(1)));

        //write out only top tfidf
        String cosOutStr = "";
        Collections.sort(wordTFIDFs);
        int idx = wordTFIDFs.size();

        for (int i = 0; i < AuthorDetection.TOP_TFIDF; i++) {
            try {
                WordTFIDF wordTFIDF = wordTFIDFs.get(--idx);
                cosOutStr = cosOutStr + wordTFIDF.getWord() + "=" + wordTFIDF.getTfidf();
            } catch (ArrayIndexOutOfBoundsException e) {
                break;
            }

        }


        mos.write("cosine", key, new Text(cosOutStr.substring(1)));

    }
}
