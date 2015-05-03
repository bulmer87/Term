package authordetect.tfidf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import authordetect.structure.TextArrayWritable;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Qiu on 4/24/15.
 * This is the reducer that calculate TF-IDF
 * Input Key: Text ---> Word
 * Input Value: TextArrayWritable ---> [ Title, Word Count, Maximum Word Count ]
 * Output Key: Text ---> Title
 * Output Value: TextArrayWritable ---> [ Word, TF-IDF Value ]
 */

public class TFIDFReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    private double totalBookCount;

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        //if the key is "!", it means values contain book count
        if (key.equals(new Text("!"))) {
            for (TextArrayWritable bookCounts : values) {
                Text bookCountText = ((Text) bookCounts.get()[0]);
                double bookCount = Double.parseDouble(bookCountText.toString());
                totalBookCount += bookCount;
            }
        }

        String word = key.toString();

        // Need to iterate values twice: One for calculate book occurance, another for emit each word to file one by one.
        // Since iterator cannot be reset, I create one more hashset.
        ArrayList<String> valArray = new ArrayList<String>();
        while (values.iterator().hasNext()) {
            TextArrayWritable val = values.iterator().next();
            valArray.add(val.toString());
        }

        double bookOccurCount = valArray.size();

        if (bookOccurCount < totalBookCount) {//if a word occurs in every book. ignore this word for space
            for (String val : valArray) {
                String[] inputVal = val.split("\\|");
                String title = inputVal[0];
                double wordCount = Double.parseDouble(inputVal[1]);
                double maxWordCount = Double.parseDouble(inputVal[2]);
                double tf = wordCount / maxWordCount;
                double idf = Math.log(totalBookCount / bookOccurCount) / Math.log(2);
                Double tfidf = tf * idf;

                Text outKey = new Text(title);
                Text[] outValText = new Text[]{new Text(word), new Text(tfidf.toString())};

                TextArrayWritable outVal = new TextArrayWritable(outValText);
                context.write(outKey, outVal);
            }
        } else {
            System.out.println("HELLO");
        }
    }
}