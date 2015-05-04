package authordetect.tfidf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import authordetect.structure.TextArrayWritable;
import authordetect.util.BookCounter;

import java.io.IOException;

/**
 * Created by Qiu on 4/24/15.
 * Mapper to calculate TF-IDF
 * Input Key: Text ---> " Title || Maximum word count "
 * Input Value: TextArrayWritable ---> [ "Word A || Word A Count", ... ]
 * Output Key: Text ---> Word
 * Output Value: TextArrayWritable ---> [ Title, Word Count, Maximum Word Count]
 */
public class TFIDFMapper extends Mapper<Text, TextArrayWritable, Text, TextArrayWritable> {


    @Override
    public void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {

        String keyStr = key.toString();
        String[] keySplit = keyStr.split("/");
        String title = keySplit[0];
        String maxCount = keySplit[1];

        //REVIEW: NEED CHECK THIS
        Writable[] wordCounts = value.get();

        // Get each word and its count info from input value.
        Text[] outValArr = new Text[3];
        for (Writable wordCount : wordCounts) {
            String str = wordCount.toString();
            String[] strSplit = str.split("/");
            String word = strSplit[0];
            String count = strSplit[1];
            Text outKey = new Text(word);

            outValArr[0] = new Text(title);
            outValArr[1] = new Text(count);
            outValArr[2] = new Text(maxCount);

            TextArrayWritable outVal = new TextArrayWritable(outValArr);

            context.write(outKey, outVal);
        }

    }

    /**
     * In cleanup process, emit the number of books that have been processed
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter(BookCounter.BOOK_COUNT);
        Long val = counter.getValue();
        Text valText = new Text(val.toString());
        // "!" has the smallest ASCII value. It will occur at the beginning in the reducer
        context.write(new Text("!"), new TextArrayWritable(new Text[]{valText}));
    }
}
