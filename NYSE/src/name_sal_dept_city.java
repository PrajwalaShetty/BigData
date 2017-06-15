import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class name_sal_dept_city{
	
	public static class MapperEx extends Mapper<Object, Text, Text, Text> {

		public void map(Object key,Text value,Context ctx) throws IOException, InterruptedException
		{
		String[] arr=value.toString().split("-");
		String city = arr[2];
		String Emp_name = arr[1];
		String Dept_name = arr[3];
		int sal = Integer.parseInt(arr[4]);
		ctx.write(new Text(city), new Text(Emp_name+ "-" +sal+ "-"+Dept_name));
		}
		}
	
	public static class ReducerEx extends Reducer<Text, Text,Text, Text>
	{
	public void reduce(Text key,Iterable<Text> itr,Context context) throws IOException, InterruptedException
	{ 
	int maxsal=0;
	String s= "";
	String sal ="";
	String Dept_name="";

	for (Text val : itr){
	String arr[] = val.toString().split("-");
	if (maxsal < Integer.parseInt(arr[1]))
	{
	maxsal = Integer.parseInt(arr[1]);
	sal = arr[1].toString();

	s = arr[0].toString();
    Dept_name = arr[3].toString();
	}

	}
	context.write(new Text(key), new Text(s.toString() +"-" + sal.toString()+ "-" +Dept_name.toString()));

	}

	}
	
	
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
	
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf);
	job.setJarByClass(name_sal_dept_city.class);
	job.setMapperClass(MapperEx.class);
	job.setReducerClass(ReducerEx.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}
}
