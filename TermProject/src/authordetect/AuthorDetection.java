package authordetect;

import authordetect.bcv.BCVMapper;
import authordetect.bcv.BCVReducer;
import authordetect.input.CombineBooksInputFormat;
import authordetect.similarity.SimilarityMapper;
import authordetect.similarity.SimilarityReducer;
import authordetect.structure.TextArrayWritable;
import authordetect.tfidf.TFIDFMapper;
import authordetect.tfidf.TFIDFReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import authordetect.util.FileSelector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Qiu on 4/24/15.
 * Main class of the project
 */
public class AuthorDetection {

    public static final int ITER = 3;
    public static final int GROUP_BY_AUTHER = 0;
    public static final int GROUP_BY_BOOK = 1;
    public static final String FIRST_TEMP_PATH = "/bulmer/output/first/";
    public static final String SECOND_TEMP_PATH = "/bulmer/output/second/";
    public static final String THIRD_TEMP_PATH = "/bulmer/output/third/";
    public static final int TOP_TFIDF = 100;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {

        Configuration configuration = new Configuration();
        FileSelector fileSelector = new FileSelector();

        DistributedRowMatrix drMatrix;
        for (int i = 0; i < ITER; i++) {
            randFileSelect(fileSelector);
            computeTFIDF(configuration, GROUP_BY_AUTHER, FileSelector.TRAINING_PATH, FIRST_TEMP_PATH + "train/" + i);// Training set
            computeTFIDF(configuration, GROUP_BY_BOOK, FileSelector.TESTING_PATH, FIRST_TEMP_PATH + "test/" + i);// Testing set
            fileSelector.moveBackFiles();
        }

        for (int i = 0; i < ITER; i++) {
            computeBCV(configuration, FIRST_TEMP_PATH + "train/" + i, SECOND_TEMP_PATH + "train/" + i);
            computeBCV(configuration, FIRST_TEMP_PATH + "test/" + i, SECOND_TEMP_PATH + "test/" + i);

//            computeSimilarity(configuration, SECOND_TEMP_PATH, THIRD_TEMP_PATH + i, i);
        }


    }

    public static void randFileSelect(FileSelector s) throws InterruptedException {
        if (s.initialize() == false) {
            System.out.println("Error in initilize");
        }
        if (s.moveFilesTo() == false) {
            System.out.println("Error in move files");
        }
//        Thread.sleep(10000);
//        if (s.moveBackFiles() == false) {
//            System.out.println("Error in move back files");
//        }
    }

    private static void computeTFIDF(Configuration configuration, int option, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

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
        FileInputFormat.setInputPaths(tfidf_Job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(tfidf_Job, true);
        tfidf_Job.setInputFormatClass(CombineBooksInputFormat.class);

        //set output path
        FileOutputFormat.setOutputPath(tfidf_Job, new Path(outputPath));

        tfidf_Job.setMapOutputKeyClass(Text.class);
        tfidf_Job.setMapOutputValueClass(TextArrayWritable.class);

        tfidf_Job.setOutputFormatClass(TextOutputFormat.class);
        tfidf_Job.setOutputKeyClass(Text.class);
        tfidf_Job.setOutputValueClass(TextArrayWritable.class);

        tfidf_Job.waitForCompletion(true);
    }

    private static void computeBCV(Configuration configuration, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Job bcv_Job = Job.getInstance(configuration, "BCV");

        bcv_Job.setJarByClass(AuthorDetection.class);

        bcv_Job.setMapperClass(BCVMapper.class);
        bcv_Job.setReducerClass(BCVReducer.class);

        bcv_Job.setMapOutputKeyClass(Text.class);
        bcv_Job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(bcv_Job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(bcv_Job, true);
        bcv_Job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(bcv_Job, new Path(outputPath));

        MultipleOutputs.addNamedOutput(bcv_Job, "euclidean", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(bcv_Job, "cosine", TextOutputFormat.class, Text.class, Text.class);

        bcv_Job.setOutputFormatClass(TextOutputFormat.class);

        bcv_Job.waitForCompletion(true);
    }

    private static void computeSimilarity(Configuration configuration, String inputPath, String outputPath, int iter) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        Job similarity_Job = Job.getInstance(configuration, "EuclidD Job");

        similarity_Job.setJarByClass(AuthorDetection.class);

        similarity_Job.setInputFormatClass(NLineInputFormat.class);

        NLineInputFormat.addInputPath(similarity_Job, new Path(inputPath));

        // Each line of the file gets sent to a mapper
        similarity_Job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

        System.out.println("SETUP NLINEINPUT FORMAT!");

        similarity_Job.setMapperClass(SimilarityMapper.class);

        similarity_Job.setMapOutputKeyClass(Text.class);
        similarity_Job.setMapOutputValueClass(Text.class);

        similarity_Job.setReducerClass(SimilarityReducer.class);

        // testing input path
        Path fourthJobInputPath = new Path(inputPath + "test/" + iter + "/part-r-00000");
        // training data path
        similarity_Job.addCacheFile(new URI(inputPath + "train/" + iter + "/part-r-00000#bcv"));

        Path fourthJobOutputPath = new Path(outputPath);

        FileInputFormat.setInputPaths(similarity_Job, fourthJobInputPath);
        FileOutputFormat.setOutputPath(similarity_Job, fourthJobOutputPath);

        similarity_Job.waitForCompletion(true);
    }
}
