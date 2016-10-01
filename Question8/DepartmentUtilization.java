import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DepartmentUtilization{
	

	private static int count = 0;
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, FloatWritable>{
		private final static IntWritable one = new IntWritable(1);
		
		private static HashMap<String,String> dayMap = new HashMap<String,String>();
		private Text word = new Text();
		
		 public static boolean isInteger(String s) {
			    try { 
			        Integer.parseInt(s); 
			    } catch(NumberFormatException e) { 
			        return false; 
			    } catch(NullPointerException e) {
			        return false;
			    }
			    return true;
		}

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
			String dept = rowData[4];
			
			Integer student_count = Integer.valueOf(rowData[l-2]);
			Integer max_capacity = Integer.valueOf(rowData[l-1]);
			FloatWritable new_count = new FloatWritable(student_count/max_capacity);
			
			String o = sem+"_"+dept;
			
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
	
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Dept utilization");
		job.setJarByClass(DepartmentUtilization.class);
		job.setMapperClass(TokenizerMapper.class);
		
		job.setCombinerClass(FloatSumReducer.class);
		job.setReducerClass(FloatSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	//	System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("count :"+count);

	}


}
