import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top5_ViableProductSubclass {

	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException
		{
		String str[] = value.toString().split(";");
		String prod_id = str[5].trim();
		String age_sales= str[2].trim()+"-"+str[8].trim();
	
		c.write(new Text(prod_id), new Text(age_sales));
		}
	}
	
	
	public static class MyPartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) 
		{
			
			String[] str = value.toString().split("-");
			
			if(str[0].equals("A"))
				return 0;
			
			else if(str[0].equals("B"))
				return 1;
			
			else if(str[0].equals("C"))
				return 2;
			
			else if(str[0].equals("D"))
				return 3;
			
			else if(str[0].equals("E"))
				return 4;
			
			else if(str[0].equals("F"))
				return 5;
			
			else if(str[0].equals("G"))
				return 6;
			
			else if(str[0].equals("H"))
				return 7;
			
			else if(str[0].equals("I"))
				return 8;
			
			else if(str[0].equals("J"))
				return 9;
			
			else
				return 10;
			
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Long,String> Product = new TreeMap<Long, String>();
		
		public void reduce(Text key, Iterable<Text> values, Context c)
		{
			 String Pd= "";
			 
			 String PdtID = "";
			 
			 String age = "";
			
			long sum = 0L;
			
			for(Text val : values)
			{
				String[] part = val.toString().split("-");
				
				 age = part[0];
				sum+=sum + Long.parseLong(part[1]);
			}
			
			Pd = key.toString() + "   " +age;;
			Product.put(sum,Pd);
		
		
		if (Product.size()>5)
		{
			Product.remove(Product.firstKey());
		}
		
		}
		
		public void cleanup(Context c) throws IOException, InterruptedException
		{
			for(Long var: Product.descendingMap().keySet())
			{
				c.write(NullWritable.get(), new Text(Product.get(var)));
			}
		}
		
	}
	 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		

		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Top 5 viable product and subclass");
		    job.setJarByClass(Top5_ViableProductSubclass.class);
		    job.setMapperClass(MyMapper.class);
		    
		    job.setPartitionerClass(MyPartitioner.class);
		    job.setReducerClass(MyReducer.class);
		    job.setNumReduceTasks(11);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}