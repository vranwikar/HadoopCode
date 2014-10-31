import java.io.*;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgRainfallAndTempReducer extends Reducer <Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException
	{
		
		double totalPercip = 0;
		double totalTempVal = 0;
		String avgPercip = null;
		String avgTemp = null;
		int count = 0;
		
		for(Text value : values)
		{
			count = count + 1;
			StringTokenizer stk = new StringTokenizer(value.toString(),",");
			totalPercip = totalPercip + Double.parseDouble(stk.nextToken());
			totalTempVal = totalTempVal + Double.parseDouble(stk.nextToken());
		}
		
		
		avgPercip = Double.toString(totalPercip/count);
		avgTemp = Double.toString(totalTempVal/count);
		context.write(key, new Text(avgPercip+", "+avgTemp));
		
		
	}
	
}
