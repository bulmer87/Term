import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;



/* Preconditions: The Path / Directory name in HDFS must be /Term/TrainerData 
 * All data must reside within this TrainerData directory this is where the files
 * will be selected from and moved from for the test data set 
 * the Path / Directory name for test data will be /Term/TestData
 * The hadoop mappers for TFIDF must point to the correct Directory
 * For the TFIDF values to correlate to the correct implementation of TFIDF*/



/* Preconditions: The Path / Directory name in HDFS must be /Term/TrainerData
 * All data must reside within this TrainerData directory this is where the files
 * will be selected from and moved from for the test data set
 * the Path / Directory name for test data will be /Term/TestData
 * The hadoop mappers for TFIDF must point to the correct Directory
 * For the TFIDF values to correlate to the correct implementation of TFIDF*/

public class FileSelector {

		public ArrayList<String> books = new ArrayList<String>();
		public boolean initialize()
        {
                try
                {
                		Process p = Runtime.getRuntime().exec("./scrpt");// this script ran will get the files that reside in /Term/TestData
                		p.waitFor();
                		try// now we input the filename into an array list for random selection
                		{
                			FileInputStream instream = new FileInputStream("test");
                			DataInputStream in = new DataInputStream(instream);
                			BufferedReader buf = new BufferedReader(new InputStreamReader(in));
                			String line;
                			while((line = buf.readLine())!=null)
                			{
                				books.add(line);
                			}
                			buf.close();
                		}catch(Exception e)
                		{
                			System.err.println("Error in reading in files: " + e.getMessage());
                		}
                }
                catch(Exception e)
                {
                        System.out.println("!!!!! " + e + " !!!!!");
                        return false;
                }
                return true;

        }
		public boolean moveFilesTo()
		{
			/*Random randomSelector = new Random();
			ArrayList<Integer> numberList = new ArrayList<Integer>();
			int selected= 0;
			for(int i=0;i<50;i++)
			{
				do
				{
					selected = randomSelector.nextInt(books.size());
				}while(numberList.contains(selected));
				try
				{
					Runtime.getRuntime().exec("/usr/local/hadoop-2.3.0/bin/hdfs dfs -mv /bulmer/Test100/" + books.get(selected) + " /Test");
					numberList.add(selected);
					Thread.sleep(500);
				}
				catch(Exception e)
	            {
					System.out.println("!!!!! " + e + "!!!!!");
	                return false;
	            }
			}
			return true;*/
			
			Random randomSelector = new Random();
			ArrayList<String> copy = new ArrayList<String>();
			copy.addAll(books);
			for(int i=0;i<50;i++)
			{
				int selected = randomSelector.nextInt(copy.size());
				try
				{
					System.out.println("The new process is started on iteration " + i);
					System.out.println("The file being copied is " + copy.get(selected));
					Process p = Runtime.getRuntime().exec("/usr/local/hadoop-2.3.0/bin/hdfs dfs -mv /bulmer/input/" + copy.get(selected) + " /Test");//movement of file to selected directory
					System.out.println("The exit status is " + p.waitFor());//get exit status 
					System.out.println("Waiting over for iteration " + i);
					copy.remove(selected);
				}
				catch(Exception e)
	            {
					System.out.println("!!!!! " + e + " !!!!!");
	                return false;
	            }
			}
			return true;
		}
		public boolean moveBackFiles()
		{
			try
			{
				Process p = Runtime.getRuntime().exec("/usr/local/hadoop-2.3.0/bin/hdfs dfs -mv /Test/* /bulmer/input");
				p.waitFor();
			}
			catch(Exception e)
            {
				System.out.println("!!!!! " + e + " !!!!!");
                return false;
            }
			return true;
		}
        public static void main(String[] args) throws InterruptedException
        {
                FileSelector s = new FileSelector();
                if(s.initialize() == false)
                {
                	System.out.println("Error in initilize");
                }
                if(s.moveFilesTo() == false)
                {
                	System.out.println("Error in move files");
                }
                Thread.sleep(10000);
                if(s.moveBackFiles() == false)
                {
                	System.out.println("Error in move back files");
                }
        }
}
