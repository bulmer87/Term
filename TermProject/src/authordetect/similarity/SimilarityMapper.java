package authordetect.similarity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by yqiu on 4/3/15.
 * Input Key: LongWritable ---> Text Offset
 * Input Value: Text ---> "Title    (BCV)" ---> "Title  Word1=TFIDF1,Word2=TFIDF2 ..."
 * Output Key: Text ---> Title1_Title2
 * Output Value: DoubleWritable ---> Similarity
 */

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {

    private RandomAccessFile randomAccessFile;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        randomAccessFile = new RandomAccessFile("authordetect", "r");
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String curLine = value.toString();
        String[] curLineSplit = curLine.split("\\t");
        String curTitle = curLineSplit[0];
        String curBCV = curLineSplit[1];

        Dictionary d1 = getDictionary(curBCV);

        randomAccessFile.seek(key.get());

        String otherLine;
        while ((otherLine = randomAccessFile.readLine()) != null) {
            String[] otherLineSplit = otherLine.split("\\t");
            String otherTitle = otherLineSplit[0];
            String otherBCV = otherLineSplit[1];
            Dictionary d2 = getDictionary(otherBCV);

            Double similarity = computeSimilarity(d1, d2);

            Text outKey = new Text(curTitle + "|" + otherTitle);
            Text outVal = new Text(similarity.toString());
            context.write(outKey, outVal);
        }
    }

    /**
     * Given a book character vector, return a dictionary structure
     *
     * @param bcv
     * @return Dictionary
     */
    private Dictionary getDictionary(String bcv) {
        String[] bcvSplit = bcv.split(",");
        Dictionary dictionary = new Dictionary();

        for (String wordtfidf : bcvSplit) {
            String word = wordtfidf.split("=")[0];
            Double tfidf = Double.parseDouble(wordtfidf.split("=")[1]);
            dictionary.put(word, tfidf);
        }

        return dictionary;
    }

    private Double computeSimilarity(Dictionary d1, Dictionary d2) {

        Double output = 0.0;

        for (String word : d1.keySet()) {
            Double tfidf1 = d1.get(word);
            Double tfidf2 = d2.get(word);
            output += Math.pow(tfidf1 - tfidf2, 2);
        }

        return Math.pow(output, 0.5);
    }
}
