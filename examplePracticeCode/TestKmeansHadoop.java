package kr.kaist.hadoop.example.kmeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

@SuppressWarnings("deprecation")
public class TestKmeansHadoop {

	
	/*
	 * Haejoon Lee modified K-menas on 2015.01.17  
	 * Reference File: https://code.google.com/p/haloop/source/browse/
	 */
	
	public static class KMeansMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, double[]> reducerOutput = new HashMap<String, double[]>();
		private Text idBuffer = new Text();
		private Text outputBuffer = new Text();
		
		public void configure(JobConf conf) {				// to get input file for K-clustering
			int iteration = conf.getInt("iteration",-1);
			String outputPath = conf.get("output_path");
		
			System.err.println("iteration number " + iteration);
			
			if (iteration <= 0) {
				// iteration == 0 initial centroids
				try {
					reducerOutput.clear();
					FileSystem dfs = FileSystem.get(conf);		

					Path path = new Path("kinfo");							// get initial centroids informations				
					System.out.println("init K centers informaiton");
					FileStatus[] files = dfs.listStatus(path);

					for (int i = 0; i < files.length; i++) {
						if (!files[i].isDir()) {
							FSDataInputStream is = dfs.open(files[i].getPath());
							String line = null;						
							
							while ((line = is.readLine()) != null) {
								
								i++;		
								reducerOutput.put(Integer.toString(i),parseStringToVector(line));								
							}
							is.close();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			} else {
				// after itertaion 2, updated centroids
				try {
					reducerOutput.clear();
					FileSystem dfs = FileSystem.get(conf);
					System.out.println("mapper current iteration " + iteration);
					String location = outputPath + "/i"
							+ (iteration - 1);
					Path path = new Path(location);
					System.out.println("reducer feedback input path: "
							+ location);

					FileStatus[] files = dfs.listStatus(path);

					for (int i = 0; i < files.length; i++) {
						if (!files[i].isDir()) {
							FSDataInputStream is = dfs.open(files[i].getPath());
							String line = null;
							while ((line = is.readLine()) != null) {
								String fields[] = line.split("\t", 2);
								reducerOutput.put(fields[0],parseStringToVector(fields[1]));													
							}
							is.close();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				if (reducerOutput.isEmpty())
					System.out.println("init error");
				}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			if (reducerOutput == null || reducerOutput.isEmpty())
				throw new IOException("reducer output is null");

			String line = value.toString();	
			String dataString = line;		
			double[] row = parseStringToVector(dataString);
						
			Iterator<String> keys = reducerOutput.keySet().iterator();
			double minDistance = Double.MAX_VALUE;
			String minID = "";

			// find the cluster membership for the data vector
			
			
			/*while(key){
			 * 
			 *  String id = keys.next();
				double[] point = reducerOutput.get(id);
			 *  calculate distance and compare min distance
			 * }
			 * 
			 * 
			 */

			idBuffer.clear();
			idBuffer.append(minID.getBytes(), 0, minID.getBytes().length);

			outputBuffer.clear();
			outputBuffer.append(dataString.getBytes(), 0,
					dataString.getBytes().length);
			output.collect(idBuffer, outputBuffer);
		}
		
		private double distance(double[] d1, double[] d2) {
			double distance = 0;
			int len = d1.length < d2.length ? d1.length : d2.length;

					
			/* for loop
			 * Euclidean distance			
			 * 
			 */
		}
	}

	// multi-dimensional Points to Vector
	private static double[] parseStringToVector(String line) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();

			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			return row;
			
		} catch (Exception e) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int size = tokenizer.countTokens();

			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;				
			}
			return row;
		}
	}

	private static void accumulate(double[] sum, double[] array) {
		
		/*
		 * for loop - summing array
		 * 
		 */
	}


	public static class KMeansReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
					
			double[] sum = null;
			long count = 0;

			sum = new double[3];
			
			
			/* while(values){
			 *  String data = values.next().toString();
				double[] row = parseStringToVector(data);
			 *  sum row array by using accumulate fuction
			 *  handling count
			 * }
			 * 
			 * 
			 */
			// generate the new means center	
			String result = "";
			for (int i = 0; i < sum.length; i++) {
				sum[i] = sum[i] / count;
				result += (sum[i] + ",");				
			}
			
			result.trim();		
			output.collect(key, new Text(result));
		}
	}


	public static void main(String[] args) throws Exception {

		for(String arg : args)
		{
			System.err.println(arg);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		long endMinor = 0;

		int specIteration = 0;
		if (args.length > 2) {
			specIteration = Integer.parseInt(args[2]);
		}	
		
		int numReducers = 3;
		if (args.length > 3) {
			numReducers = Integer.parseInt(args[3]);
		}			

		int currentIteration = 0;
		long start = System.currentTimeMillis();
		while (currentIteration < specIteration) {
			JobConf conf = new JobConf(TestKmeansHadoop.class);
			conf.setJobName("K-means");

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			conf.setMapperClass(KMeansMapper.class);			
			conf.setReducerClass(KMeansReducer.class);
			conf.setCombinerClass(KMeansReducer.class);
			
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setInt("iteration",currentIteration);
			conf.set("output_path", outputPath);
			conf.setNumReduceTasks(numReducers);
			conf.setSpeculativeExecution(false);		
			
			FileInputFormat.setInputPaths(conf, new Path(inputPath));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/i"
					+ currentIteration));
			
			JobClient.runJob(conf);
			currentIteration++;
			// Increase iterations
			endMinor = System.currentTimeMillis();
			System.out.println(currentIteration+"-iteration  " + (endMinor - start) / 1000 + "s");
		}
		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");
	}
}