import java.io.IOException;
//import java.util.*;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TFIDFStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
	{
		String[] valArray = value.toString().split("@@@");
		String[] keyArray = valArray[0].split("::");
		context.write(new Text(keyArray[1]),new Text(keyArray[0] + "=" + valArray[1].trim()));
	}
}