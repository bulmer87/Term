package TFIDFForTrainingSet;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TFIDFStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	public String Author ="";
	boolean read = false;
	IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
	{
		String line = value.toString();
		if(read == false)
		{
			if(line.startsWith("Author:"))
			{
					Author = (line.substring(8)).trim();
					return;
			}
			if(line.startsWith("*** START OF"))
			{
				read = true;
				return;
			}
		}
		if(read==true)
		{
			StringTokenizer tokenizer = new StringTokenizer(line, " \t");
			while(tokenizer.hasMoreTokens())
			{
				String currentToken = tokenizer.nextToken().toLowerCase().replaceAll("\\p{Punct}", "").trim();
				context.write(new Text(currentToken + "::" + Author + "@@@"),one);
			}
		}
	}
}