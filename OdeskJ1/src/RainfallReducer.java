import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RainfallReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> 
{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	throws IOException, InterruptedException
	{
		double percipSum = 0;
		for(DoubleWritable value : values)
		{
			percipSum = percipSum + value.get();
		}
		context.write(key, new DoubleWritable(percipSum));
		
	}
	
}
