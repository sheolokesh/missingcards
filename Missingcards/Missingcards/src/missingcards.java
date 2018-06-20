import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class missingcards {
	public static class map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] elements = line.split(",");

			Text tx = new Text(elements[0]);
			Text tx1 = new Text(elements[1]);

			IntWritable it = new IntWritable(Integer.parseInt(elements[1]));
				context.write(tx, it);	
		}	
		}
	
	public static class Reduce extends
	Reducer<Text, IntWritable, Text, Text> {
		Text miss = new Text();
public void reduce(Text key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException {
	
	String s = new String();
	ArrayList<Integer> kvals = new ArrayList<Integer>();
	for (IntWritable value : values) {
		  kvals.add(value.get());
	}
	for (int i=1;i<=13;i++){
		if(!kvals.contains(i))
		{
			s=String.valueOf(i);
			miss.set(s);
			context.write(key, miss);
			
		}
	}
	
}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "missing cards for poker");
		job.setJarByClass(missingcards.class); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		job.setMapperClass(map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}
	
	
	
	
	
	}















