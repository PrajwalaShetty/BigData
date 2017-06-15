package udfhive;

import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Word_Count extends UDF{
	public int evaluate(Text text)
	{
		int count=0;
		if(text==null) 
		return 0;
		StringTokenizer itr = new StringTokenizer(text.toString());
		   while(itr.hasMoreTokens()){
			   itr.nextToken();
			   count++;
		   }
		return count;
		
	}
}
