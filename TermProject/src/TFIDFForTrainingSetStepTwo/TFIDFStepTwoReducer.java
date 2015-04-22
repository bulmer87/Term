import java.io.IOException;
import java.util.*;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TFIDFStepTwoReducer extends Reducer<Text, Text, Text, Text>
{	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
	{
		List<String> wordsInBook = new ArrayList<String>();
		int max = -1000000;
		for(Text val : values)
		{
			wordsInBook.add(val.toString());
			String[] findMax = val.toString().split("=");
			if(Integer.valueOf(findMax[1]) > max)
			{
				max = Integer.valueOf(findMax[1]);
			}
		}
		for(String word : wordsInBook)
		{
			String[] remake = word.split("=");
			context.write(new Text(remake[0]+"::"+key+"@@@"), new Text(remake[1]+"/"+max));
		}
	}
}
