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



public class CourseDay {
	

	private static int count = 0;
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private static ArrayList<String> dayIndex;
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
		 
			@Override
		protected void setup(Context context){

				dayMap.put("M", "Monday");
				dayMap.put("T", "Tuesday");
				dayMap.put("W", "Wednesday");
				dayMap.put("R", "Thursday");
				dayMap.put("F", "Friday");
				dayMap.put("S", "Saturday");
				dayMap.put("U", "Sunday");
				dayIndex = new ArrayList<String>();
				dayIndex.add("M");
				dayIndex.add("T");
				dayIndex.add("W");
				dayIndex.add("R");
				dayIndex.add("F");
				dayIndex.add("S");
				dayIndex.add("U");
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String itr = value.toString();
			String[] rowData = itr.split(",");
			
			int l = rowData.length;
			if(rowData[5].isEmpty()||rowData[5].equalsIgnoreCase("Unknown")||rowData[6].isEmpty()||rowData[6].equalsIgnoreCase("UNKWN")||rowData[7].isEmpty()||rowData[7].equalsIgnoreCase("Unknown")||rowData.length>11)
			{
				return;
			}
			
				String Hall = rowData[5].split(" ")[0];
				String Sem = rowData[3];
				String o;
				String days = rowData[6].trim();
	//			System.out.println(days);
				if(days.contains("-"))
				{
					String stDay = days.substring(0,1);
					String endDay = days.substring(2,3);
					
					int stIn = -1;
					int endIn = -1;
					if(dayIndex.contains(stDay))
					{
							stIn = dayIndex.indexOf(stDay);
							
					}
					if(dayIndex.contains(endDay))
					{
							
							endIn = dayIndex.indexOf(endDay);
					}
					
					int i=0;
					if(stIn==-1 || endIn==-1)
						return;
					for(i=stIn;i<=endIn && i<dayIndex.size();i++)
					{
							o = Sem+"_"+dayMap.get(dayIndex.get(i));
							word.set(o);
							context.write(word, new IntWritable(1));						
					}
					
				}
				else
				{
					for(int i=0;i<days.length();i++)
					{
						if(dayMap.containsKey(days.substring(i,i+1)))
						{
							o = Sem+"_"+dayMap.get(days.substring(i,i+1));
							word.set(o);
							context.write(word, new IntWritable(1));
							count++;
						}
					}
					
				}
				
			
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
		Job job = Job.getInstance(conf, "days and number of courses held");
		job.setJarByClass(CourseDay.class);
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
