package input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import structure.TextArrayWritable;

import java.io.IOException;

/**
 * Created by Qiu on 4/24/15.
 * This is the combine file input format
 * It combines multiple books into a larger chunk (64mb) as mapper's input
 * This is used for enhance performance.
 */

public class CombineBooksInputFormat extends CombineFileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {

        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        CombineFileRecordReader<Text, TextArrayWritable> combineFileRecordReader
                = new CombineFileRecordReader<Text, TextArrayWritable>(combineFileSplit, context, CombineBooksReader.class);
        try {
            combineFileRecordReader.initialize(combineFileSplit, context);
        } catch (InterruptedException e) {
            System.err.println("Failed to initialize combine file record reader");
            System.err.println(e.getMessage());
        }
        return combineFileRecordReader;
    }
}
