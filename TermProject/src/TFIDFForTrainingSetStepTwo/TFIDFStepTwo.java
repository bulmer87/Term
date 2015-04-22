import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
public class TFIDFStepTwo extends Configured implements Tool 
{

	  public static void main(String args[]) throws Exception 
	  {
	    int res = ToolRunner.run(new TFIDFStepTwo(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception 
	  {
		  Path inputPath = new Path(args[0]);
		  Path outputPath = new Path(args[1]);
		  Configuration conf = getConf();
		  @SuppressWarnings("deprecation")
		  Job job = new Job(conf, this.getClass().toString());
		  FileInputFormat.setInputPaths(job, inputPath);
		  FileOutputFormat.setOutputPath(job, outputPath);
		  job.setJobName("TFIDF StepTwo");
		  job.setJarByClass(TFIDFStepTwo.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setMapperClass(TFIDFStepTwoMapper.class);
		  job.setReducerClass(TFIDFStepTwoReducer.class);

	    return job.waitForCompletion(true) ? 0 : 1;
	  }
}