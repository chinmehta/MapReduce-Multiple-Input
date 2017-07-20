package pack;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MultipleInputs {
	
	

	public static class Mapper1 implements Mapper<LongWritable, Text, Text, Text>
	{
		
		private String commonkey, status1, fileTag = "s1~";

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			// TODO Auto-generated method stub
			
			  String v = value.toString();
			  
			  StringTokenizer st=new StringTokenizer(v, ",");
			
			  while(st.hasMoreTokens())
			  {
			   commonkey = st.nextToken().trim();
			   st.nextToken();
			   st.nextToken();
			   st.nextToken();
			   st.nextToken();
			    status1 = st.nextToken().trim();
			   
			   
			    output.collect(new Text(commonkey), new Text(fileTag+" "+status1));
			  }
		}
		
	}
	
	
	public static class Mapper2 implements Mapper<LongWritable, Text, Text, Text>
	{
		
	
	  
		private String commonkey, status2, fileTag = "s2~";

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter report) throws IOException {
		// TODO Auto-generated method stub
		
		
		 String line = value.toString();
	  
		 StringTokenizer st=new StringTokenizer(line, ",");
		 
		 while(st.hasMoreTokens())
		 {
	      commonkey = st.nextToken().trim();
	      st.nextToken();
	      st.nextToken();
	      st.nextToken();
	      st.nextToken();
	      
	      status2 = st.nextToken().trim();
	      
	      output.collect(new Text(commonkey), new Text(fileTag+""+status2));
	 
		 }
	}
	}
	

	public static class Reduce implements Reducer<Text, Text, Text, Text>
	{
		private String status1, status2;
		private String s1, s2;
		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			// TODO Auto-generated method stub
			
			
			while (values.hasNext())
			{
				
				
				String currvalue=values.next().toString();
				
				StringTokenizer st=new StringTokenizer(currvalue, "~");
				
				while(st.hasMoreTokens())
				{
					s1=st.nextToken().trim();
					s2=st.nextToken().trim();
				
				
			

					 output.collect(new Text(s1), new Text(s2));
		}
			}
	}
	}


	public static void main(String args[]) throws IOException
	{
		JobConf conf = new JobConf(MultipleInputs.class);
		conf.setJobName("SMS Reports");
	 
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		
	    conf.setReducerClass(Reduce.class);
		
		
	
		org.apache.hadoop.mapred.lib.MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Mapper1.class);
		
		org.apache.hadoop.mapred.lib.MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Mapper2.class);
		
		
	FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		conf.setOutputFormat(TextOutputFormat.class);
		JobClient.runJob(conf);
		
	}
	
}

	

