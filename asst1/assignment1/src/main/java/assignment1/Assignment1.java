/**
 *@author Xiuping WU
 *z5172649
 *COMP9313 Asst1
 */
package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * This class solves the problem posed for Assignment1
 *
 */
public class Assignment1 {
	private static int ngram;                       			/*The value N for the ngram*/
	private static int mincount;					   			/*The minimum count for an ngram*/
	
	/**
	 * Maps input key/value pairs to set of intermediate key/value pairs
	 */
	public static class TokenizerMapper 
			extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text(); 						/*Output key*/
		
		/**
		 * Map method
		 *@param key/value  input key and value to the mapper
		 *@param context  store the intermediate outputs of running task. 
		 */
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
            FileSplit fileSplit = (FileSplit)context.getInputSplit();		
            String currFile = fileSplit.getPath().getName();	/*Name of currently processed file*/
			String[] words = value.toString().split(" ");		/*String array storing tokens*/
			for (int i = 0; i < words.length-(ngram-1); i++) {	
				String temp = words[i];							/*Currently processed word */
				for (int j = i+1; j < (i+ngram); j++) {
					temp += " ";								/*Add new words to form ngram*/
					temp += words[j];
				}
				word.set(temp);							/*Assign ngram to Text 'word'*/
				Text t = new Text(currFile);			/*Change String to Text output*/
				
				/*
				 * Key-value pair in form of (word, "filename")
				 */
				context.write(word, t);					
			}
		}
	}
	
	/**
	 * Aggregate input key/value pairs to final key/value pairs
	 */
	public static class IntSumReducer
    		extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		
		/**
		 *reduce method
		 *@param key/files input key-value pair in form of 
		 *				   (word,list<fielname, filename, filename...>)
		 */
		public void reduce(Text key, Iterable<Text> files,//intwrite->text
                    Context context
                    ) throws IOException, InterruptedException {
			
			/*
			 * Count the number of occurrence of each ngram
			 */
			int cnt = 0;
			ArrayList<String> fileslist = new ArrayList<String>();		/*List for filenames*/
	        for (Text fil : files) {
	        	fileslist.add(fil.toString());
	        }
	        cnt = fileslist.size();
	        
	        /*
	         *Output satisfied ngrams
	         */
	        if (cnt >= mincount) {
		        String textresult = Integer.toString(cnt)+" ";
		        
		        /*
		         * Remove the redundant filenames
		         */
		        HashSet<String> h = new HashSet<String>(fileslist);
		        fileslist.clear();
		        fileslist.addAll(h);
		        Collections.sort(fileslist);						/*Sort filenames*/
		        for (int i = 0; i < fileslist.size(); i++) {
		        	textresult += " " + fileslist.get(i);			
		        }
		        Text t = new Text(textresult);
		        result.set(t);
		        
		        /*
		         *Key-value pair in form of (word,"ngramcount filename filename...")
		         */
		        context.write(key, result);
	        }
		}
	}
	
	public static void main(String[] args) throws Exception {
		ngram = Integer.parseInt(args[0]);
		mincount = Integer.parseInt(args[1]);
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Assignment1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[2]));
	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}