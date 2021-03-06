import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Top4_10Prod {

	
	public static class MyMapper extends Mapper<LongWritable, Text,  Text, LongWritable>
		{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
			String [] str = value.toString().split(";");
			String prodID= str[5];
			long sales = Long.parseLong(str[8]);
			c.write(new Text(prodID),new LongWritable (sales));	
		}
			
		   }
	public static class MyReducer extends Reducer< Text, LongWritable,  Text, LongWritable>{
	  private TreeMap<Long, String> maxprod = new TreeMap<Long, String>();
		    
	        
			String prodID = "";
			
			
			public void reduce(Text key, Iterable<LongWritable> values, Context c)
			{
				long sum = 0L;
				for(LongWritable val: values)
				{
					sum+=val.get();
				}
				prodID = key.toString();
				maxprod.put(sum,prodID);
				
				if(maxprod.size()>5)
				{
					maxprod.remove(maxprod.firstKey());
				}
			}
			public void cleanup(Context c) throws IOException, InterruptedException
			{
				for(Long l:maxprod.descendingMap().keySet()){
					c.write(new Text(maxprod.get(l)), new LongWritable(l));
				}
			}
			
			
		}
			
	
	
			
			public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

				 Configuration conf = new Configuration();
				    //conf.set("name", "value")
				    
				    Job job = Job.getInstance(conf, "Top Products");
				    job.setJarByClass(Top4_10Prod.class);
				    job.setMapperClass(MyMapper.class);
				    
				    //job.setPartitionerClass(MyPart.class);
				    job.setReducerClass(MyReducer.class);
				    //job.setNumReduceTasks(0);
				    job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(LongWritable.class);
				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
    }

}