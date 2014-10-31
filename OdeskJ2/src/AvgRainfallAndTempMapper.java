import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AvgRainfallAndTempMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	private static final int MISSING = 9999;
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		
		String line = value.toString();
		StringTokenizer stk = new StringTokenizer(line," ");
		String stationID = stk.nextToken().replace(" ", "");
		String zipCode = stk.nextToken().replace(" ", "");
		String lat = stk.nextToken().replace(" ", "");
		String lon = stk.nextToken().replace(" ", "");       
		String temp = stk.nextToken().replace(" ", "");  
		String percip = stk.nextToken().replace(" ", "");
		String humid = stk.nextToken().replace(" ", "");
		String year = stk.nextToken().replace(" ", ""); 
		String month = stk.nextToken().replace(" ", ""); 
		int yearValue = Integer.parseInt(year);
		
		if(yearValue >= 1950)
		{
			String newKey = zipCode + ", " + year + ", ";
			String percipTempValue = percip + "," + temp;
			context.write(new Text(newKey), new Text(percipTempValue));
		}
	}
	
}
