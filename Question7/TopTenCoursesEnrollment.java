import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TopTenCoursesEnrollment {
	

	private static int count = 0;
	public static class TokenizerMapper
	extends Mapper<Object, Text, NullWritable, Text>{
		private final static IntWritable one = new IntWritable(1);
		
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


		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		 
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			
			String itr = value.toString();
			String[] rowData = itr.split(",");
			
			int l = rowData.length;
			if(rowData[5].isEmpty()||rowData[5].equalsIgnoreCase("Unknown")||rowData[6].isEmpty()||rowData[6].equalsIgnoreCase("UNKWN")||rowData[7].isEmpty()||rowData[7].equalsIgnoreCase("Unknown")||Integer.valueOf(rowData[l-2])<0)
			{
				return;
			}

			
			String Sem = rowData[3];
			String course = rowData[8];
			
//			String o = Sem+"_"+course;
			IntWritable courseEnroll = new IntWritable(Integer.valueOf(rowData[l-2]));
			String o = Sem+"_"+course+";"+Integer.valueOf(rowData[l-2]);
			word.set(o);
			repToRecordMap.put(courseEnroll.get(), new Text(o));
			
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
				
				
				String str = value.toString();
				
				repToRecordMap.put(Integer.parseInt(str.substring(str.indexOf(";")+1,str.length())),new Text(value));				
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
			
		}
	}
	
	

	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "year utilization");
		job.setJarByClass(TopTenCoursesEnrollment.class);
		job.setMapperClass(TokenizerMapper.class);
		

		job.setReducerClass(TopTenReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	//	System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("count :"+count);

	}


}
