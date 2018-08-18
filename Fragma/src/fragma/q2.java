package fragma;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class q2 {
	public static Map<String,String> movie = new HashMap<String,String>();
	public static Map<String,Double> sortpurpose=new HashMap<String,Double>();
	
	public static class map1 extends Mapper<LongWritable,Text,Text,Text>
	{
		//private IntWritable one=new IntWritable(1);
		private Text movieid=new Text();
		double rate=0.0;
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		String line=value.toString();
		String[] split=line.split("::");
		if(split.length == 4)
		{
			movieid.set(split[1]);
			
			context.write(movieid,new Text(split[2]));
		}
	
		
		}
	}
	
	
	public static class red1 extends Reducer<Text,Text,Text,Text>
	{
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			      throws IOException, InterruptedException {
			int count=0;
			double sum=0;
			double avg=0;
			for(Text var:values)
			{
				count+=1;
				sum+=Double.parseDouble(var.toString());
			}
			if(count>39)
			{
			avg=(sum/count);
			sortpurpose.put(key.toString(), avg);
			context.write(key,new Text(key.toString()+"::"+avg));
			}
		}
	}
	
	
	public static class map2 extends Mapper<Object,Text,Text,Text>
	{
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] split=line.split("::");
			context.write(new Text(split[0]),new Text( split[1]));
			
		}	
	}
	
	public static class red2 extends Reducer<Text,Text,Text,Text>
	{
		private Text finalvalue=new Text();
		private Text finalkey=new Text();
		static int loop=0;
		public void reduce(Text key, Iterable<Text> values, Context context)
			      throws IOException, InterruptedException {
			
			if(loop<20)
			{
			Map<String,Double> sortmap=new TreeMap(new compbyvalue(sortpurpose));
			sortmap.putAll(sortpurpose);
			
			for(Map.Entry<String,Double> each_value : sortmap.entrySet())
			{
				loop=loop+1;
				finalkey.set(each_value.getKey());
				finalvalue.set(each_value.getValue().toString());
				context.write(finalkey,finalvalue);
				if(loop==20)
				{
					break;
				}
				
			}
			
			}	
		}
	}
	
	public static class compbyvalue implements Comparator {

		Map map;
		public compbyvalue(Map map) {
			this.map = map;
		}

		public int compare(Object keyA, Object keyB) {

			Double valueA = (Double) map.get(keyA);
			Double valueB = (Double) map.get(keyB);

			if ( valueB >= valueA) {
				return 1;
			} else {
				return -1;
			}
		}
	}

	
		
	public static void main(String args[]) throws Exception
	{
		
		if (args.length != 3) 
		{
			System.err.println("wrong argument");
			System.exit(-1);
		}
		
		
		Configuration conf=new Configuration();
		Job job1=new Job(conf,"first job");
		job1.setMapperClass(map1.class);
		job1.setReducerClass(red1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setJarByClass(q2.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		//FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));
		int code = job1.waitForCompletion(true) ? 0 : 1;
		int code2 = 1;
		
		
		if(code == 0)
		{
			/*Map<String,Integer> sortmap=new TreeMap(new compbyvalue(sortpurpose));
			sortmap.putAll(sortpurpose);*/
			
			Configuration conf1=new Configuration();
			Job job2 = new Job(conf1,"Second job");
			
			job2.setJarByClass(q2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapperClass(map2.class);
			job2.setReducerClass(red2.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			System.exit(job2.waitForCompletion(true)?0:1);
			
		}
		
		
	}
	
	
	}
