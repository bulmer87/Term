package authordetect.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import authordetect.structure.TextArrayWritable;
import authordetect.structure.WordCountMap;
import util.BookCounter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Qiu on 4/24/15.
 * This record reader takes a single book as input.
 * Output key: Text ---> " Title(Author)  / Maximum Word Count "
 * Output value: TextArray ---> [ "Word A / Word A Count", ... ]
 */

public class SingleBookReader extends RecordReader<Text, TextArrayWritable> {

    private LineReader lineReader;
    private String title;
    private Text currentLine = new Text(""); //key
    private Text key; //key is book info
    private TextArrayWritable value; //value is words count array
    private long start, end, currentPos;
    private String filename;
    private boolean hasTitleOrAuthor = true;
    private boolean hasStart = true;
    private WordCountMap wordCountMap;
    private TaskAttemptContext context;
    private boolean isFinish = false;

    public SingleBookReader(TaskAttemptContext context) {
        wordCountMap = new WordCountMap();
        this.context = context;
    }

    /**
     * @param inputSplit
     * @param context    the information about the task
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) inputSplit;
        Configuration configuration = context.getConfiguration();

        // get the option from configuration:
        // 0 for group by author, 1 for group by book
        int option = configuration.getInt("GROUP_OPTION", 0);

        Path path = split.getPath();
        filename = path.getName();
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        lineReader = new LineReader(inputStream, configuration);

        //initial start point and end point
        start = split.getStart();
        end = start + split.getLength();

        inputStream.seek(start);
        if (start != 0) {
            start += lineReader.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
        }

        start += lineReader.readLine(currentLine);

        prepareToScanBook(option);
    }

    /**
     * Preparation to process actual book content
     * Skip license content, project description etc.
     * Reduce bandwidth usage
     *
     * @throws IOException
     */
    private void prepareToScanBook(int opt) throws IOException {
        //get the title of the book
        while (!containsTitleOrAuthor(currentLine, opt)) {
            try {
                int readBytes = lineReader.readLine(currentLine);
                //if does not find line of title, return
                if (readBytes == 0 || !hasTitleOrAuthor) {
                    hasTitleOrAuthor = false;
                    return;
                }
                //update cursor of linereader
                start += readBytes;
            } catch (IOException e) {
                hasTitleOrAuthor = false;
                System.err.println("Error when retriving title for book ---> " + filename);
                System.err.println(e.getMessage());
                return;
            }

        }

        //get book start line
        while (!isBookStart(currentLine)) {
            try {
                int readBytes = lineReader.readLine(currentLine);
                //if does not find book start line, return
                if (readBytes == 0 || !hasStart) {
                    hasStart = false;
                    return;
                }
                //update cursor of linereader
                start += readBytes;
            } catch (IOException e) {
                hasStart = false;
                System.err.println("Error when retriving start line for book ---> " + filename);
                System.err.println(e.getMessage());
                return;
            }
        }

        currentPos = start;
    }

    private boolean containsTitleOrAuthor(Text line, int option) throws IOException {
        String lineString = line.toString();
        String target;

        if (option == 0) {
            target = "Author";
        } else {
            target = "Title";
        }

        if (lineString.startsWith(target)) {
            title = lineString.split(":")[1].substring(1);
            return true;
        } else {
            return false;
        }
    }

    private boolean isBookStart(Text line) {
        String lineString = line.toString();
        return lineString.toLowerCase().contains("start") && lineString.toLowerCase().contains("gutenberg");
    }

    /**
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!filename.endsWith("txt")) {//only process txt file
            return false;
        }

        if (currentPos >= end || !hasTitleOrAuthor || !hasStart) {//false if finishes processing the split
            return false;
        }

        if (!isFinish) {
            processBookContent();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }


    @Override
    public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    private void processBookContent() throws IOException {

        currentPos += lineReader.readLine(currentLine);
        String currentLineStr = currentLine.toString().toLowerCase();

        //Processing book content line by line. And update the word map
        while (!isFinish) {
            String[] words = currentLineStr.split(" ");
            //write all words into the word map
            for (String word : words) {
                word = word.trim().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                if (!word.equals("")) {
                    wordCountMap.put(word, 1);
                }
            }
            //detect book end
            if (currentLineStr.contains("end") && currentLineStr.contains("gutenberg")) {
                isFinish = true;

                //update counter which stores the book count
                Counter counter = context.getCounter(BookCounter.BOOK_COUNT);
                counter.increment(1);
            }
            currentPos += lineReader.readLine(currentLine);
            currentLineStr = currentLine.toString().toLowerCase();
        }


        //convert word map to text array
        int arrayLen = wordCountMap.entrySet().size();
        Iterator<Map.Entry<String, Integer>> iterator = wordCountMap.entrySet().iterator();
        int maxCount = 0, count;
        String word, wordCount;
        Text[] wordArray = new Text[arrayLen];

        for (int i = 0; i < arrayLen; i++) {
            Map.Entry<String, Integer> entry = iterator.next();
            word = entry.getKey();
            count = entry.getValue();
            wordCount = word + "/" + count;
            wordArray[i] = new Text(wordCount);

            if (count > maxCount) {//get the maximum word count as well
                maxCount = count;
            }
        }

        key = new Text(title + "/" + maxCount);
        value = new TextArrayWritable(wordArray);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

}
