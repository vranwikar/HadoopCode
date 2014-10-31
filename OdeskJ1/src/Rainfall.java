
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rainfall {
   

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Rainfall <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(Rainfall.class);
		job.setJobName("Rainfall");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RainfallMapper.class);
		job.setReducerClass(RainfallReducer.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
