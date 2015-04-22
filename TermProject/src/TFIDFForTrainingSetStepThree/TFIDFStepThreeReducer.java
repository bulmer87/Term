package TFIDFForTrainingSetStepThree;
import java.io.IOException;
import java.util.*;
//import java.lang.Math;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TFIDFStepThreeReducer extends Reducer<Text, Text, Text, Text>
{	
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
	{
		int numberOfBooks = context.getConfiguration().getInt("NumberOfDocs", 0);
		//String[] TitleNames = context.getConfiguration().getStrings("Name");
		List<String> books = new ArrayList<String>();
		for(Text val : values)
		{ 
			books.add(val.toString());
		}
		int booksWordAppearedIn = books.size();
		for(String book: books)
		{
			String[] splitIt = book.split("=");
			String[] splitToGet = splitIt[1].split("/");
			double TF = Double.valueOf(splitToGet[0]) / Double.valueOf(splitToGet[1]);
			double IDF = Math.log(Double.valueOf(numberOfBooks)/Double.valueOf(booksWordAppearedIn))/Math.log(2);
			double TFIDF = TF * IDF;
			if (TFIDF == 0.0)
				continue;
			String TFIDFS = Double.toString(TFIDF);
			context.write(new Text(key+"::"+splitIt[0]+"@@@"),new Text(TFIDFS));
		}
	}
}