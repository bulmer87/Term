import input.CombineBooksInputFormat;
import structure.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import tfidf.TFIDFMapper;
import tfidf.TFIDFReducer;

import java.io.IOException;

/**
 * Created by Qiu on 4/24/15.
 */
public class AuthorDetection {

    public static Path firstTempPath = new Path("/output/tmp/1");

    public static void main(String[] args) {

        Configuration configuration = new Configuration();

    }

    private static void computeTFIDF(Configuration configuration, int option, Path path) throws IOException, ClassNotFoundException, InterruptedException {

        // option = 0 ---> Group by author
        // option = 1 ---> Group by book
        configuration.setInt("GROUP_OPTION", option);

        //store an variable of total books count
        Job tfidf_Job = Job.getInstance(configuration, "TF-IDF");

        //set main class
        tfidf_Job.setJarByClass(AuthorDetection.class);

        //set mapper/combiner/reducer
        tfidf_Job.setMapperClass(TFIDFMapper.class);
        tfidf_Job.setReducerClass(TFIDFReducer.class);

        //set the combine file size to maximum 64MB
        tfidf_Job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        tfidf_Job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);

        //set input path
        FileInputFormat.setInputPaths(tfidf_Job, path);
        FileInputFormat.setInputDirRecursive(tfidf_Job, true);
        tfidf_Job.setInputFormatClass(CombineBooksInputFormat.class);

        //set output path
        FileOutputFormat.setOutputPath(tfidf_Job, firstTempPath);

        tfidf_Job.setMapOutputKeyClass(Text.class);
        tfidf_Job.setMapOutputValueClass(TextArrayWritable.class);

        tfidf_Job.setOutputFormatClass(TextOutputFormat.class);
        tfidf_Job.setOutputKeyClass(Text.class);
        tfidf_Job.setOutputValueClass(TextArrayWritable.class);

        tfidf_Job.waitForCompletion(true);
    }

}
