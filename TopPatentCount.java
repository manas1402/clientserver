

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopPatentCount {

	private static int count;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopPatent count");

		count = Integer.parseInt(args[2]);

		job.setJarByClass(TopPatentCount.class);
		job.setMapperClass(TopPatentCountMapper.class);
		job.setCombinerClass(TopPatentCountsReducer.class);
		job.setReducerClass(TopPatentCountsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TopPatentCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable mapOutValue = new IntWritable(1);
		private Text mapOutKey = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] valArray = value.toString().split("\t");
			mapOutKey.set(new Text(valArray[valArray.length - 1]));
			context.write(mapOutKey, mapOutValue);
		}
	}

	public static class TopPatentCountsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			if (count <= 0) {
				System.out.println(" Count is negetive " + count);
			} else {
				writeOut(key, values, context);
				count--;
			}

		}

	}

	private static void writeOut(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		IntWritable result = new IntWritable();
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
