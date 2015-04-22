package AuthorsGetter;
import java.io.IOException;
//import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AuthorsGetterReducer extends Reducer<Text, Text, Text, Text>
{	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
	{
		String s = "";
		for(Text t : values)
		{
			s += t.toString().trim() + "\t";
		}
		context.write(key, new Text(s));
	}
}