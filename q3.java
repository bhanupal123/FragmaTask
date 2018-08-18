package test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
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
public class q3 {

	public static Map<Integer,String> profession = new HashMap<Integer,String>();
	
		
	
//read ratings.dat file	
public class mapper1 extends Mapper<Object,Text,Text,Text>
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

public class mapper2 extends Mapper<Object,Text,Text,Text>
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



public class mapper1andmapper2joinreducer extends Reducer<Text,Text,Text,Text>
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


public class mapper3 extends Mapper<Object,Text,Text,Text>
{
private Text mid=new Text();
private Text outmap3=new Text();

public void map(Object key,Text values,Context context) throws IOException
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

public class mapper4 extends Mapper<Object,Text,Text,Text>
{
private Text mid=new Text();
private Text outmap4=new Text();

public void map(Object key,Text values,Context context) throws IOException
{
	String line=values.toString();
	String[] data=line.split("::");
	if(data.length==4)
	{
		mid.set(data[0]);
		outmap4.set("mvon"+data[1]+"::"+data[2]);
		context.write(mid,outmap4);
		
	}
	
	
}

}
















	public static void main(String args[])
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

		
	}


}












}