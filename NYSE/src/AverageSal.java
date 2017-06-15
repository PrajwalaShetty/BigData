import java.io.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class AverageSal {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, FloatWritable>
	   {
		public void map(LongWritable key, Text values, Context ctx) throws IOException, InterruptedException
		{
			String[] str = values.toString().split("-");
			String city = str[2];
			Float sal = Float.parseFloat(str [4]);
			ctx.write(new Text(city), new FloatWritable(sal));
		}
	   }
	public static class MyReducer extends Reducer<Text, Text, Text, FloatWritable>
	{
		 public void reduce(Text key, Iterable<FloatWritable> values, Context c) throws IOException, InterruptedException
			{
				
				
				Float total = 0.0f;
				int count=0;
				
				for(FloatWritable val: values)
				{
					total+=val.get();
					count++;
				}
				
				float avg = total/count;
				
				
				c.write(key,new FloatWritable(avg));
			}
		}
		
			
		 
	   	  
			
	  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		  
		  Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Average salary");
		    job.setJarByClass(AverageSal.class);
		    job.setMapperClass(MapClass.class);
		    
		    job.setReducerClass(MyReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    
		  }
}


