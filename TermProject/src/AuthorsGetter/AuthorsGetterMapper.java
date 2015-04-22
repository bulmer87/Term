package AuthorsGetter;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class AuthorsGetterMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public String Title ="";
	public String Author="";
	public String FileName="";
	boolean getTitle = true;
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
	{
		String line = value.toString();
		FileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(getTitle == true)
		{
			if(line.startsWith("Title:"))
			{
				Title = (line.substring(7)).trim();
				return;
			}
			if(line.startsWith("Author:"))
			{
				Author = line.substring(8).trim();
				return;
			}
			
			if(line.startsWith("*** START OF"))
			{
				context.write(new Text(Title), new Text(Title + " @ " + FileName));
				getTitle = false;
				return;
			}
			
		}
	}
}