package TFIDFForTrainingSet;
import java.io.IOException;
//import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TFIDFStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
	{
		int total = 0;
		for (IntWritable value : values) 
		{
			total += value.get();
		} 
		context.write(key, new IntWritable(total));
	}
}