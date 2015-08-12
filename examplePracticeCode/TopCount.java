
/*******************************************************************************
 *  File : TopWordCount.java
 *  Developer: Haejoon Lee Computer Science 20143533
 *  Goal: Big Data group assignment
 ********************************************************************************/
package kr.kaist.hadoop.example.wordcount;

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
public class TopCount {

	public static class TopMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"%']";

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
			StringTokenizer itr = new StringTokenizer(cleanLine);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken().trim());
				output.collect(word, one);
			}
		}

	}

	public static class TopMap1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] arrValue = line.split("\\t");
			//System.out.println("Key- " + new Text(arrValue[0]) + " Value- " + arrValue[1]);
			output.collect(new Text(arrValue[0]), new IntWritable(Integer.parseInt(arrValue[1])));			
		}
	}

	public static class TopReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		private TreeMap<IntWritable, Text> countMap = new TreeMap<IntWritable, Text>();
		private OutputCollector<Text, IntWritable> collector = null;

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			int sum = 0;
			collector = output;

			while (values.hasNext()) {
				sum += values.next().get();
			}
			countMap.put(new IntWritable(sum), new Text(key));
			if (countMap.size() > 20) {
				countMap.remove(countMap.firstKey());
			}
		}

		public void close() throws IOException {
			for (IntWritable key : countMap.keySet()) {
				collector.collect(countMap.get(key), key);
			}
		}

	}

	public static class Combiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext())
				sum += values.next().get();

			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		
		long start = System.currentTimeMillis();
		
		Path interPath = new Path("interResult");
		JobConf conf = new JobConf(TopCount.class);
		conf.setJobName("TopNCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(TopMap.class);
		if (args.length == 3) {
			if (new String(args[2]).equals("combiner"))
				conf.setCombinerClass(Combiner.class);
			else
				System.err.print("<Usage: <input> <output> combiner>\n");
		}
		conf.setReducerClass(TopReduce.class);
		conf.setNumReduceTasks(5);
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, TopMap.class);
		FileOutputFormat.setOutputPath(conf, interPath);

		JobClient.runJob(conf);

		// Job 2
		JobConf conf1 = new JobConf(TopCount.class);
		conf.setJobName("TotalNCount");
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);
		conf1.setMapperClass(TopMap1.class);
		conf1.setReducerClass(TopReduce.class);
		conf1.setNumReduceTasks(1);

		MultipleInputs.addInputPath(conf1, interPath, TextInputFormat.class, TopMap1.class);
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

		JobClient.runJob(conf1);
		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");

	}

}
