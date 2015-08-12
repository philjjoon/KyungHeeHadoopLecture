package kr.kaist.hadoop.example.wordcount;


/*******************************************************************************
 *  File : TopWordCount.java
 *  Developer: Haejoon Lee Computer Science 20143533
 *  Goal: Big Data group assignment
 ********************************************************************************/

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

@SuppressWarnings("unused")
public class TestTopCount {

	public static class TopMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"%']";

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
			StringTokenizer itr = new StringTokenizer(cleanLine);			
			
			// while(StringTokenizer Class.hasMoreTokens())
				// word.set()				
				// output.collect(Key, Values)			
		}

	}

	public static class TopMap1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			// String Class Split by Tab			
			// output.collect(Key, Values)
		}
	}

	public static class TopReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		private TreeMap<IntWritable, Text> countMap = new TreeMap<IntWritable, Text>();
		//
		private OutputCollector<Text, IntWritable> collector = null;

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			int sum = 0;
			collector = output;

			//while(); sum
			
			countMap.put(new IntWritable(sum), new Text(key));
			//countMap.put(IntWritable(), Text());
			if (countMap.size() // ) {
				// removing the others
			}
		}

		public void close() throws IOException {
			// for loop from countMap.keySet()
			// OutputCollector.collect(Key, Count);			
		}

	}

	public static class Combiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		/*
		 * public void reduce() {
		 *
		 *
		 *}
		 * 
		 */
	}

	public static void main(String[] args) throws Exception {

		
		long start = System.currentTimeMillis();
		
		Path interPath = new Path("interResult");
		JobConf conf = new JobConf(TestTopCount.class);
		conf.setJobName("TopNCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		//conf.setMapperClass();
		if (args.length == 3) {
			if (new String(args[2]).equals("combiner")){
				//conf.setCombinerClass();
				System.err.print("Set Combiner");
			}else
				System.err.print("<Usage: <input> <output> combiner>\n");
		}
		//conf.setReducerClass();
		conf.setNumReduceTasks(5);
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, TopMap.class);
		FileOutputFormat.setOutputPath(conf, interPath);

		JobClient.runJob(conf);

		// Job 2
		JobConf conf1 = new JobConf(TestTopCount.class);
		conf.setJobName("TotalNCount");
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);
		//conf1.setMapperClass();
		//conf1.setReducerClass();

		conf1.setNumReduceTasks(1);
		
		// MultipleInputs.addInputPath(conf1, **** , TextInputFormat.class, ****);
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

		JobClient.runJob(conf1);		
		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");

	}

}
