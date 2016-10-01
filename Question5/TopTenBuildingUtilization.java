import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class TopTenBuildingUtilization {
	

	private static int count = 0;
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, FloatWritable>{
		private final static IntWritable one = new IntWritable(1);
		
		private Text word = new Text();

		 
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			
			String itr = value.toString();
			
			String[] rowData = itr.split(",");
			
			int l = rowData.length;
			if(rowData[5].isEmpty()||rowData[5].equalsIgnoreCase("Unknown")||rowData[6].isEmpty()||rowData[6].equalsIgnoreCase("UNKWN")||rowData[7].isEmpty()||rowData[7].equalsIgnoreCase("Unknown")||Integer.valueOf(rowData[l-2])<0||Integer.valueOf(rowData[l-1])==0)
			{
				return;
			}

			String sem = rowData[3];
			String course = rowData[8];
			String building = rowData[5].split(" ")[0];
			
			Integer student_count = Integer.valueOf(rowData[l-2]);
			Integer max_capacity = Integer.valueOf(rowData[l-1]);
			FloatWritable new_count = new FloatWritable(student_count/max_capacity);
			
			String o = sem+"_"+building;
			
			word.set(o);
			context.write(word, new_count);
			
			
		}
		
	}
	
	
	public static class FloatSumReducer
	extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context
				) throws IOException, InterruptedException {
			float sum = 0;
			
			for(FloatWritable val : values)
			{
				sum = sum +val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
		
 public static class Mapper2
	  extends Mapper<Object, Text, NullWritable, Text>{
	  	Text word=new Text();

	private TreeMap<Float, Text> repToRecordMap = new TreeMap<Float, Text>();
	
	public void map(Object key, Text value, Context context
	               ) throws IOException, InterruptedException {
		
		String[] fields=value.toString().split("\\t");
		
		String o = value.toString();
		word.set(o);
		repToRecordMap.put(Float.parseFloat(fields[1]), new Text(o));
		
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
		
		private TreeMap<Float, Text> repToRecordMap = new TreeMap<Float, Text>();
		
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			

			for(Text value: values)
			{
				
				
				String fields[] = value.toString().split("\\t");
				
				repToRecordMap.put(Float.parseFloat(fields[1]),new Text(value));				
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
		Job job = Job.getInstance(conf, "top ten building utilization");
		job.setJarByClass(TopTenBuildingUtilization.class);
		job.setMapperClass(TokenizerMapper.class);
		
		job.setReducerClass(FloatSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("Temp"));
		job.waitForCompletion(true);
	//	System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	    Configuration conf2 = new Configuration();
		 Job job2 = Job.getInstance(conf2, "top ten building by utilization");
		 job2.setJarByClass(TopTenBuildingCourses.class);
		 job2.setMapperClass(Mapper2.class);
		 
		 job2.setReducerClass(TopTenReducer.class);
		 job2.setOutputKeyClass(NullWritable.class);
		 job2.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job2, new Path("Temp"));
		 FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		 System.exit(job2.waitForCompletion(true) ? 0 : 1);
		   
		
		
		
		
		System.out.println("count :"+count);

	}


}
