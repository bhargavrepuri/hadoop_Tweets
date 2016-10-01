import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DepartmentCoursesNumber {
	

	private static int count = 0;
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
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
			
			dayMap.put("M", "Monday");
			dayMap.put("T", "Tuesday");
			dayMap.put("W", "Wednesday");
			dayMap.put("R", "Thursday");
			dayMap.put("F", "Friday");
			dayMap.put("S", "Saturday");
			dayMap.put("U", "Sunday");
			ArrayList<String> dayIndex = new ArrayList<String>();
			dayIndex.add("M");
			dayIndex.add("T");
			dayIndex.add("W");
			dayIndex.add("R");
			dayIndex.add("F");
			dayIndex.add("S");
			dayIndex.add("U");
			
			String itr = value.toString();
			String[] rowData = itr.split(",");
			
			int l = rowData.length;
			if(rowData[5].isEmpty()||rowData[5].equalsIgnoreCase("Unknown")||rowData[6].isEmpty()||rowData[6].equalsIgnoreCase("UNKWN")||rowData[7].isEmpty()||rowData[7].equalsIgnoreCase("Unknown")||Integer.valueOf(rowData[l-2])<0)
			{
				return;
			}

			
			String Sem = rowData[3];
			String dept = rowData[4];
			String o = Sem+"_"+dept;
			IntWritable courseEnroll = new IntWritable(Integer.valueOf(rowData[l-2]));
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
	
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "year utilization");
		job.setJarByClass(DepartmentCoursesNumber.class);
		job.setMapperClass(TokenizerMapper.class);
		
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	//	System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("count :"+count);

	}


}
