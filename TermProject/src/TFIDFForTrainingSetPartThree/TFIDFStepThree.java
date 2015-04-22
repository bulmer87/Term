import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
public class TFIDFStepThree extends Configured implements Tool 
{

	  public static void main(String args[]) throws Exception 
	  {
	    int res = ToolRunner.run(new TFIDFStepThree(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception 
	  {
		  Path filePath = new Path(args[0]);
		  Path inputPath = new Path(args[1]);
		  Path outputPath = new Path(args[2]);
		  Configuration conf = getConf();
		  ArrayList<String> Titles = new ArrayList<String>();
		  try
		  {
              Path pt=new Path(args[0]);
              FileSystem fs = FileSystem.get(new Configuration());
              BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
              String line;
              line=br.readLine();
              while (line != null)
              {
            	  	  //System.out.println(line);
            	  	  if(!line.trim().isEmpty())
            	  		  Titles.add(line.trim());
                      line=br.readLine();
              }
		  }catch(Exception e){}
		  final int numberOfFiles = Titles.size();
		  System.out.print("This is file length" + numberOfFiles + "\n");
		  conf.setInt("NumberOfDocs", numberOfFiles);
		  //conf.setStrings("Names", Names);
		  @SuppressWarnings("deprecation")
		  Job job = new Job(conf, this.getClass().toString());

		  FileInputFormat.setInputPaths(job, inputPath);
		  FileOutputFormat.setOutputPath(job, outputPath);

		  job.setJobName("TFIDF StepThree");
		  job.setJarByClass(TFIDFStepThree.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  //job.setOutputFormatClass(TextOutputFormat.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);

		  job.setMapperClass(TFIDFStepThreeMapper.class);
		  job.setReducerClass(TFIDFStepThreeReducer.class);

	    return job.waitForCompletion(true) ? 0 : 1;
	  }
}