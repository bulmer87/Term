package authordetect.input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import authordetect.structure.TextArrayWritable;

import java.io.IOException;

/**
 * Created by Qiu on 4/24/15.
 * This reader reads records from combine books input format
 * Output key is the current line
 * Output value is book title
 */
public class CombineBooksReader extends RecordReader {

    private int index;
    private SingleBookReader bookReader;

    public CombineBooksReader(CombineFileSplit split, TaskAttemptContext context, Integer index) {
        super();
        this.index = index;
        this.bookReader = new SingleBookReader(context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(index),
                combineFileSplit.getOffset(index), combineFileSplit.getLength(), combineFileSplit.getLocations());
        bookReader.initialize(fileSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return bookReader.nextKeyValue();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return bookReader.getCurrentKey();
    }

    @Override
    public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return bookReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bookReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        bookReader.close();
    }
}
