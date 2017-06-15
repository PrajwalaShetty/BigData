import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ReduceJoinEx {
 public static class CustsMapper extends Mapper<LongWritable,Text,Text,Text>
 {
	 public void map(LongWritable key,Text value,Context con)
	 {
		 
	 }
 }
	public static void main(String[] args) {
		
	}

}
