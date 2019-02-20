import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduce {

  public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	
    	StringTokenizer tokenizer = new StringTokenizer(line, "\t");
    	
    	int count = 0;
    	String img_id = "";
    	int upvotes = 0;
    	int downvotes = 0;
    	int comments = 0;
    	
    	while (tokenizer.hasMoreTokens()) {
    		String tmp = tokenizer.nextToken();
    		
    		if(count == 1) {
    			img_id = tmp;
    		} else if(count == 2) {
    			upvotes = Integer.parseInt(tmp);
    		} else if(count == 3) {
    			downvotes = Integer.parseInt(tmp);
    		} else if(count == 4) {
    			comments = Integer.parseInt(tmp);
    		}
    		
    	count++;
    	
      }
    	
    	count = 0 ;
    	int final_val = upvotes+downvotes+comments;
		value.set(img_id);
		context.write(value, new IntWritable(final_val));
    	
    }
  }

  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable x : values) {
        sum += x.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "RedditMapReduce");
        
    job.setJarByClass(MapReduce.class);
    
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    Path outputPath = new Path(args[1]);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    outputPath.getFileSystem(conf).delete(outputPath, true);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
//	  String line = "1351794987	17333	35966	33738	973	Kiss Cam";
//  	
//  	StringTokenizer tokenizer = new StringTokenizer(line, "\t");
//  	
//  	int count = 0;
//  	String img_id = "";
//  	int upvotes = 0;
//  	int downvotes = 0;
//  	int comments = 0;
//  	
//  	while (tokenizer.hasMoreTokens()) {
//  		String tmp = tokenizer.nextToken();
//  		
//  		if(count == 1) {
//  			img_id = tmp;
//  		} else if(count == 2) {
//  			upvotes = Integer.parseInt(tmp);
//  		} else if(count == 3) {
//  			downvotes = Integer.parseInt(tmp);
//  		} else if(count == 4) {
//  			comments = Integer.parseInt(tmp);
//  		}
//  		
//  	count++;
//  	
//    }
//  	
//  	count = 0 ;
//  	int final_val = upvotes+downvotes+comments;
//  	System.out.println(img_id + " " + final_val);
//  	System.out.println(upvotes);
//	  
  }
}
