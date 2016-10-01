import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class TopTenTimings {
	

	private static int count = 0;
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		
		private Text word = new Text();

		 
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			
			String itr = value.toString();
			String[] rowData = itr.split(",");
			
			if(rowData[5].isEmpty()||rowData[5].equalsIgnoreCase("Unknown")||rowData[6].isEmpty()||rowData[6].equalsIgnoreCase("UNKWN")||rowData[7].isEmpty()||rowData[7].equalsIgnoreCase("Unknown"))
			{
				return;
			}
			
			String Sem = rowData[3];
			String time = rowData[7];
			String o = Sem+"_"+time;			
			word.set(o);
			context.write(word, one);
			
		}
		
	}
	
	
	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
		
 public static class Mapper2
	  extends Mapper<Object, Text, NullWritable, Text>{
	  	Text word=new Text();

	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
	
	public void map(Object key, Text value, Context context
	               ) throws IOException, InterruptedException {
		
		String[] fields=value.toString().split("\\t");
		
		String o = value.toString();
		word.set(o);
		repToRecordMap.put(Integer.parseInt(fields[1]), new Text(o));
		
		if(repToRecordMap.size()>10)
		{
			repToRecordMap.remove(repToRecordMap.firstKey());
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Text t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
 
 public static class TopTenReducer
	extends Reducer<NullWritable,Text,NullWritable,Text> {
		private IntWritable result = new IntWritable();
		
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			

			for(Text value: values)
			{
				String fields[] = value.toString().split("\\t");
				
				repToRecordMap.put(Integer.parseInt(fields[1]),new Text(value));				
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				String[] fields = t.toString().split("\\t");
				context.write(NullWritable.get(), t);
			}
			
		}
}
 
 


	

	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "year utilization");
		job.setJarByClass(TopTenTimings.class);
		job.setMapperClass(TokenizerMapper.class);
		
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("Temp"));
		job.waitForCompletion(true);
	//	System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	    Configuration conf2 = new Configuration();
		 Job job2 = Job.getInstance(conf2, "top ten course timings");
		 job2.setJarByClass(TopTenTimings.class);
		 job2.setMapperClass(Mapper2.class);
		 
		 job2.setReducerClass(TopTenReducer.class);
		 job2.setOutputKeyClass(NullWritable.class);
		 job2.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job2, new Path("Temp"));
		 FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		 System.exit(job2.waitForCompletion(true) ? 0 : 1);
		   
		
		

	}


}
