package EMR_DSPS.mainPack;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstPart {
	private static String locationOfStopWords;
	public static class MapperClass  extends Mapper<LongWritable,Text,IdentityKey,IntWritable> { 
		private String language;
		private IntWritable decade = new IntWritable();
		private IntWritable appearance = new IntWritable();
		private Text wordOne = new Text();
		private Text wordTwo = new Text();
		private Text tagMark = new Text("$");
		private Text totalDecadeWords = new Text("TAGTotal");
		private StopWords stopWords;

		public void setup(Context context) { //Called once at the beginning of the task.
			language = context.getConfiguration().get("lang", "heb");
			if (context.getConfiguration().getBoolean("PCHadoop",true))
				stopWords = new PCHadoopStopWords(context
						.getConfiguration()
						.get("locationOfStopWords", "1"));
			else
				stopWords = new EMRStopWords(context
						.getConfiguration()
						.get("locationOfStopWords", "1"));
		}

		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException { //Context for spitting out valaue key pairs.
			//value = hello world	2005	5	0	0
			StringTokenizer iterator = new StringTokenizer(value.toString()); 
			String wordOnestring = iterator.nextToken();
			String wordTwostring = iterator.nextToken();
			wordOnestring = lowerCaseOnly(wordOnestring,language);
			wordTwostring = lowerCaseOnly(wordTwostring,language);
			if (wordOnestring.length() == 0 || wordTwostring.length() == 0) return;
			wordOne.set(wordOnestring); //hello
			wordTwo.set(wordTwostring); //world
			int yr = Integer.parseInt(iterator.nextToken()); //2005
			decade.set((yr/10)*10); //2000
			int amountPerYear = Integer.parseInt(iterator.nextToken()); //5
			appearance.set(amountPerYear);
			if (stopWords != null && !stopWords.contains(wordOne.toString()) && !stopWords.contains(wordTwo.toString()) ) {
				//    | Key | 
				//	| Result |
				context.write(
						new IdentityKey(new FirstKey(wordOne,decade), tagMark), 
						appearance);	// Word 1 and number of appearance in decade        		
				context.write(
						new IdentityKey(new FirstKey(wordTwo,decade), tagMark), 
						appearance);	// Word 2 and number of appearance in decade
				context.write(
						new IdentityKey(new FirstKey(tagMark,decade), totalDecadeWords), 
						new IntWritable(amountPerYear*2)); // Total amount of words in the decade without stop words.
			}
			else if (stopWords == null)
				System.out.println("SAY WHAT NWORD?");
		}
	}


	//The Combiner class is used in between the Map class and the Reduce class to reduce the volume of data transfer between Map and Reduce.
	public static class CombinerClass extends Reducer<IdentityKey, IntWritable,IdentityKey, IntWritable> {

		private IntWritable combinerOutput = new IntWritable();
		public void reduce(IdentityKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sumOfValues = 0; 	
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()) {
				sumOfValues += iterator.next().get();
			}
			combinerOutput.set(sumOfValues);
			context.write(key, combinerOutput);
		}
	}


	public static class PartitionerClass extends Partitioner<IdentityKey,IntWritable> {
		
		public int getPartition(IdentityKey key, IntWritable value, int numPartitions) {
			return Math.abs(key.getFirstKey().hashCode()) % numPartitions; 
		}  
	}


	public static class ReducerClass  extends Reducer<IdentityKey,IntWritable,IdentityKey,IdentityKeyValue> {
		private int c1;
		private int totalNumDecade;
		public void setup(Context context) {
			totalNumDecade = 0;
			c1 = 0;
		}
		public void reduce(IdentityKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sumOfValues = 0; 	
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()) {
				sumOfValues += iterator.next().get();
			}
			FirstKey firstK = key.getFirstKey();
			if (key.getTag().toString().equals("TAGTotal")) {
				totalNumDecade = sumOfValues;
				context.write(
						new IdentityKey(new FirstKey(firstK.getWord(), firstK.getDecade()), new Text("1TotalWords")), 
						new IdentityKeyValue(totalNumDecade));
			}
			else if (key.getTag().toString().equals("$")) {
				c1 = sumOfValues;
				context.write(
						new IdentityKey(new FirstKey(firstK.getWord(), firstK.getDecade()), new Text("2SingleDecadeWord")), 
						new IdentityKeyValue(c1));
			}     	
		}
	}
	
	public static String lowerCaseOnly(String word, String lang) {
		if (lang.equals("eng"))
			return word.replaceAll("[^A-Za-z0-9]","").toLowerCase();
		if (lang.equals("heb"))
			return word.replaceAll("[^\u05D0-\u05EA]",""); 
		return "";
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("PCHadoop", false);
		conf.set("locationOfStopWords", args[1]);
		conf.set("lang", args[2]);
		Job job = new Job(conf, "FirstPartJob");
		job.setJarByClass(FirstPart.class);
		job.setMapperClass(MapperClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(CombinerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(IdentityKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IdentityKey.class);
		job.setOutputValueClass(IdentityKeyValue.class);
	//	job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(20);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("1FirstPartOutput"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}