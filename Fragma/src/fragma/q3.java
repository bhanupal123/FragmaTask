package fragma;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import fragma.top_ten_movie.compbyvalue;
public class q3 {

public static Map<Integer,String> profession = new HashMap<Integer,String>();
	
		
	
//read ratings.dat file	
public static class mapper1 extends Mapper<Object,Text,Text,Text>
{
	Text uid=new Text();
	Text outmap1=new Text();
	
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException
	{
		String line=value.toString();
		String[] data=line.split("::");
		if(data.length==4)
		{
		uid.set(data[0]);
		outmap1.set("Rate"+data[1]+"::"+data[2]);
		context.write(uid,outmap1);
		}
		
	}


}

public static class mapper2 extends Mapper<Object,Text,Text,Text>
{
	Text uid=new Text();
	Text outmap2=new Text();
	String professional,ageRange=null;
	
	
	
	public String getAgeRange(String age)
	{
		int a=Integer.parseInt(age);
		if(a<18)
		{return null;}
		else if(a>=18 && a<=35)
		{return "18-35";}
		else if(a>=36 && a<=50)
		{return "36-50";}
		else if(a>=51)
		{return "50+";}
		else
		{return null;}
		
		
	}
	
	
	
	
	public  void map(Object key,Text value,Context context) throws IOException,InterruptedException
	{
		String line=value.toString();
		String[] data=line.split("::");
		if(data.length==5)
		{
			ageRange=getAgeRange(data[2]);
			if(ageRange!=null)	
			{
			uid.set(data[0]);
			outmap2.set("User"+ageRange+"::"+profession.get(Integer.parseInt(data[3])));
			context.write(uid,outmap2);
			}
		}
		
	}
	
}



public static class mapper1andmapper2joinreducer extends Reducer<Text,Text,Text,Text>
{
	public ArrayList<Text> userlist=new ArrayList<Text>();
	public ArrayList<Text> ratelist=new ArrayList<Text>();
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
	{
		userlist.clear();
		ratelist.clear();
		for(Text result:values)
		{
			if(result.toString().substring(0,4).equals("User"))
			{
				userlist.add(new Text(result.toString().substring(4)));
			}
			else if(result.toString().substring(0,4).equals("Rate"))
			{
				ratelist.add(new Text(result.toString().substring(4)));
			}
			
		}
		
		if(!userlist.isEmpty()&&!ratelist.isEmpty())
		{
			for(Text every_user:userlist)
			{
				for(Text every_rate:ratelist)
				{
					context.write(every_user,every_rate);
				}
			}
		}
		
	}


}

//handle output of reducer
public static class mapper3 extends Mapper<Object,Text,Text,Text>
{
private Text mid=new Text();
private Text outmap3=new Text();

public void map(Object key,Text values,Context context) throws IOException, InterruptedException
{
	String line=values.toString();
	String[] data=line.split("::");
	if(data.length==4)
	{
		mid.set(data[2]);
		outmap3.set("usmv"+data[0]+"::"+data[1]+"::"+data[3]);
		context.write(mid,outmap3);
		
	}
	
	
}

}

//read movie dataset
public static class mapper4 extends Mapper<Object,Text,Text,Text>
{
private Text mid=new Text();
private Text outmap4=new Text();

public void map(Object key,Text values,Context context) throws IOException, InterruptedException
{
	String line=values.toString();
	String[] data=line.split("::");
	if(data.length==3)
	{
		mid.set(data[0]);
		outmap4.set("mvon"+data[1]+"::"+data[2]);
		context.write(mid,outmap4);
		
	}
	
	
}

}


public static class mapper3andmapper4reducer extends Reducer<Text,Text,Text,Text>
{	Text outred = new Text();
	private ArrayList<Text> movielist=new ArrayList<Text>();
	private ArrayList<Text> ratelist=new ArrayList<Text>();
	
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException
	{
		movielist.clear();
		ratelist.clear();
		for(Text result:values)
		{
			if(result.toString().substring(0,4).equals("usmv"))
			{
				ratelist.add(new Text(result.toString().substring(4)));
			}
			else if(result.toString().substring(0,4).equals("mvon"))
			{
				movielist.add(new Text(result.toString().substring(4)));
			}
		}
		
		if (!movielist.isEmpty() && !ratelist.isEmpty()) 
		{
			for (Text moviesData : movielist) 
			{
				String[] data = moviesData.toString().split("::");
				String[] genres=data[1].split("\\|");
				for (Text ratingData : ratelist) 
				{	
					for (String genre : genres) 
					{
					outred.set(genre);
					context.write(outred, ratingData);
					}
				}
			}

		}

	}
	
}


public static class mapper5 extends Mapper<Object,Text,Text,Text>
{
	private Text k=new Text();
	private Text v=new Text();
	public void map(Object key,Text values,Context context)throws IOException,InterruptedException
	{
		String line=values.toString();
		String[] data=line.split("::");
		if(data.length==4)
		{
			k.set(data[2]+"::"+data[1]);
			v.set(data[0]+"::"+data[3]);
			context.write(k,v);
			
		}
		
	}
	

}


public static class finalred extends Reducer<Text,Text,Text,Text>
{
	Double sum=0.0;
	Integer count=0;
	private Map<String, String> map = new HashMap<String, String>();
	private Map<String,Double> Map = new HashMap<String,Double>();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		Double sum=0.0;
		Integer count=0;
	for(Text result:values)
	{
		String line=result.toString();
		String[] data=line.split("::");
		if(data.length==2)
		{	
			String genre=data[0];
			Double rating=Double.parseDouble(data[1]);
			
			String s=map.get(genre);
			if(s!=null)
			{
				String[] temp=s.split("::");
				sum=Double.parseDouble(temp[1])+rating;
				count=Integer.parseInt(temp[2])+1;
			}
			else
			{
				sum=rating;
				count=1;
					
			}
			map.put(genre,genre+"::"+sum+"::"+count);
		}
		
	}
	
	for (Map.Entry<String, String> entry : map.entrySet())
	{
		String var = entry.getValue();
		String[] div=var.split("::");
		Double sumr=Double.parseDouble(div[1]);
		Integer countr=Integer.parseInt(div[2]);
		Double average = sumr / countr;
		Map.put(entry.getKey(),average);
	}
		StringBuilder sb = new StringBuilder();
		int finalcount = 0;
		Map<String,Double> sortmap=new TreeMap(new compbyvalue(Map));
		sortmap.putAll(Map);
		for(Map.Entry<String,Double> each_value : sortmap.entrySet())
		{
		if (finalcount < 5) 
		{
		sb.append(each_value.getKey() +",");
		finalcount++;
		} else
		{
		break;
		}
		}
		context.write(key, new Text(sb.toString().substring(0, sb.toString().lastIndexOf(","))));
		}
		}

//hadoop fs -rm -R /user/hduser/fragma/ml-1m/temp123 /user/hduser/fragma/ml-1m/temp234 /user/hduser/fragma/ml-1m/final
//hadoop jar q3.jar fragma.q3 /user/hduser/fragma/ml-1m/ratings.dat /user/hduser/fragma/ml-1m/users.dat /user/hduser/fragma/ml-1m/temp123 /user/hduser/fragma/ml-1m/temp234 /user/hduser/fragma/ml-1m/movies.dat /user/hduser/fragma/ml-1m/final
	





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







	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		profession.put(new Integer(0),"other or not specified");
		profession.put(new Integer(1),"academic/educator");
		profession.put(new Integer(2),"artist");
		profession.put(new Integer(3),"clerical/admin");
		profession.put(new Integer(4),"college/grad student");
		profession.put(new Integer(5),"customer service");
		profession.put(new Integer(6),"doctor/health care");
		profession.put(new Integer(7),"executive/managerial");
		profession.put(new Integer(8),"farmer");
		profession.put(new Integer(9),"homemaker");
		profession.put(new Integer(10),"K-12 student");
		profession.put(new Integer(11),"lawyer");
		profession.put(new Integer(12),"programmer");
		profession.put(new Integer(13),"retired");
		profession.put(new Integer(14),"sales/marketing");
		profession.put(new Integer(15),"scientist");
		profession.put(new Integer(16),"self-employed");
		profession.put(new Integer(17),"technician/engineer");
		profession.put(new Integer(18),"tradesman/craftsman");
		profession.put(new Integer(19),"unemployed");
		profession.put(new Integer(20),"writer");

		
		if (args.length != 6) 
		{
			System.err.println("wrong argument");
			System.exit(-1);
		}
		
		
		
			Configuration conf = new Configuration();
			Job job0=new Job(conf,"first job");
			job0.setJarByClass(q3.class);
			job0.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			TextOutputFormat.setOutputPath(job0, new Path(args[2]));
			job0.setOutputKeyClass(Text.class);
			job0.setOutputValueClass(Text.class);
			job0.setReducerClass(mapper1andmapper2joinreducer.class);
			MultipleInputs.addInputPath(job0, new Path(args[0]), TextInputFormat.class, mapper1.class);
			MultipleInputs.addInputPath(job0, new Path(args[1]), TextInputFormat.class, mapper2.class);
			int code = job0.waitForCompletion(true) ? 0 : 1;
			int code2 = 1;
			
			
			if (code == 0) 
			{
			Job job1 = new Job(conf,"second job");
			job1.setJarByClass(q3.class);
			job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			TextOutputFormat.setOutputPath(job1, new Path(args[3]));
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setReducerClass(mapper3andmapper4reducer.class);
			MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class,
			mapper3.class);
			MultipleInputs.addInputPath(job1, new Path(args[4]), TextInputFormat.class, mapper4.class);
			code2 = job1.waitForCompletion(true) ? 0 : 1;
			}
			
			
			if (code2 == 0) 
			{
			Job job = new Job(conf,"final job");
			job.setJarByClass(q3.class);
			job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			FileInputFormat.addInputPath(job, new Path(args[3]));
			FileOutputFormat.setOutputPath(job, new Path(args[5]));
			job.setMapperClass(mapper5.class);
			job.setReducerClass(finalred.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
			
			
			}
	}
		
		



