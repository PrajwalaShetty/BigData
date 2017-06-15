import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HighestSal {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException
		{
			String[] str = value.toString().split("-");
			String dept = str[3];
			String name_sal = str[1]+","+ str[4];
			c.write(new Text(dept), new Text(name_sal));
		}
	}
	

	public static class MyReducer extends Reducer<Text, Text, Text, FloatWritable>
	{
		private Text outputKey = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context c) throws IOException, InterruptedException
		{
			
		 float max = 0;	
		 for (Text val : values)
		{
			 String [] str = val.toString().split(",");
			 if (Float.parseFloat(str[1])>max)
			 {
				 max = Float.parseFloat(str[1]);
				 String key1 =  key.toString()+'-'+ str[0];
				 outputKey.set(key1);
			 }
			 
	      	
		}
		 c.write(outputKey,new FloatWritable(max));
			//c.write(key, values);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Highest salary in dept");
		    job.setJarByClass(HighestSal.class);
		    job.setMapperClass(MyMapper.class);
		    
		    //job.setPartitionerClass(MyPart.class);
		    job.setReducerClass(MyReducer.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);


	}

}
