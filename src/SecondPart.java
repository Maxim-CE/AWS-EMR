package EMR_DSPS.mainPack;


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondPart {
    public static class MapperClass  extends Mapper<LongWritable,Text,IdentityKey,IntWritable> { 	 	
    	private IntWritable decade = new IntWritable();
    	private IntWritable appearances = new IntWritable();
    	private Text firstWord = new Text();
    	private Text secondWord = new Text();
    	private Text mapTag = new Text("$$");
    	private StopWords stopWords;
    	private String choosenLangauge;
    	
    	@Override
    	public void setup(Context context) {
    		choosenLangauge = context.getConfiguration().get("lang", "heb");
    		
    		//decide from which location to get stopWords
    		boolean configCheck = context.getConfiguration().getBoolean("PCHadoop",true);
    		String resource = context.getConfiguration().get("locationOfStopWords","1");
    		if (configCheck) stopWords = new PCHadoopStopWords(resource);
    		else stopWords = new EMRStopWords(resource);
    	}
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {   	
        		//System.out.println("Step2: "+value.toString());
        		
        		//set iterator
            	StringTokenizer itr = new StringTokenizer(value.toString()); 
            	
            	//set words
            	String firstWordstring = itr.nextToken();
	        	String secondWordstring = itr.nextToken();
	        	
	        	//set words to lower case
	        	firstWordstring = lowerCaseOnly(firstWordstring,choosenLangauge);
	        	secondWordstring = lowerCaseOnly(secondWordstring,choosenLangauge);
	        	
	        	//check for empty word
	        	if (firstWordstring.length() == 0 || secondWordstring.length() == 0) return;
	        	firstWord.set(firstWordstring);
	        	secondWord.set(secondWordstring);
            	
	        	//set decade and appearances
	        	int exactYear = Integer.parseInt(itr.nextToken());
            	decade.set((exactYear/10)*10);
            	appearances.set(Integer.parseInt(itr.nextToken()));
            	
            	//check for stopWords
            	if (stopWords == null) {
            		System.out.println("stopWord is NULL - error");
            	}
            			//    | Key | 
          				//	| Result |
            	else if (	stopWords != null && 
            				!stopWords.contains(firstWord.toString()) && 
            				!stopWords.contains(secondWord.toString())) {
            			context.write(
            					new IdentityKey(new FirstKey(firstWord,decade), mapTag), 
            					appearances);
            			context.write(
            					new IdentityKey(new FirstKey(secondWord,decade), mapTag), 
            					appearances);
            			context.write(
            					new IdentityKey(new FirstKey(firstWord,decade), secondWord), 
            					appearances);  
            	}
        }
    }
    
    public static class CombinerClass extends Reducer<IdentityKey, IntWritable,IdentityKey, IntWritable> {
    	private IntWritable combinerResult = new IntWritable();
		@Override
        public void reduce(IdentityKey identityKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       		int sumOfValues = 0; 	
       		Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()) {
				sumOfValues += iterator.next().get();
			}
        	combinerResult.set(sumOfValues);
        	context.write(identityKey, combinerResult);
		}
    }
    
    //Output of combiner goes here.
    public static class PartitionerClass extends Partitioner<IdentityKey,IntWritable> {      
        public int getPartition(IdentityKey identityKey, IntWritable value, int numberOfPartitions) {
        	return Math.abs(identityKey.getFirstKey().hashCode()) % numberOfPartitions; 
        }  
    }  
    
    public static class ReducerClass  extends Reducer<IdentityKey,IntWritable,IdentityKey,IdentityKeyValue> {
    	private int c1;
    	
    	public void setup(Context context) {
    		c1 = 0;
    	}
    	
    	// All appearances of the same collocation in the same decade
        public void reduce(IdentityKey identityKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       		int c12 = 0; 
        	FirstKey firstKey = identityKey.getFirstKey();
        	Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()) {
				c12 += iterator.next().get();
			}
        	if (identityKey.getTag().toString().equals("$$")) {
            	c1 = c12;
        	}
            else { 	
            	context.write(	
            			
            			//write IdentityKey  [(firstKey,decade),"3Collocation"]
            			new IdentityKey(new FirstKey(identityKey.getTag(), firstKey.getDecade()), new Text("3Collocation")),
            			
            			//write IdentityKeyvalue [(word,c1),c12]
            			new IdentityKeyValue(firstKey.getWord(), new IntWritable(c1), new IntWritable(c12))); 
            }
        }
    }
    
    public static String lowerCaseOnly(String word, String lang) {
        if (lang.equals("eng")) return word.replaceAll("[^A-Za-z0-9]","").toLowerCase();
        if (lang.equals("heb")) return word.replaceAll("[^\u05D0-\u05EA]",""); 
        return "";
    }
    
    public static void main(String[] args) throws Exception {
        
    	//configuration
    	Configuration conf = new Configuration();
        conf.setBoolean("PCHadoop", false);
        conf.set("locationOfStopWords", args[1]);
        conf.set("lang", args[2]);
        
        //set MapReduce 
        Job secondPartJob = new Job(conf, "secondPartJob");
        secondPartJob.setJarByClass(SecondPart.class);
        secondPartJob.setMapperClass(MapperClass.class);
        secondPartJob.setPartitionerClass(PartitionerClass.class);
        secondPartJob.setCombinerClass(CombinerClass.class);
        secondPartJob.setReducerClass(ReducerClass.class);
        secondPartJob.setMapOutputKeyClass(IdentityKey.class);
        secondPartJob.setMapOutputValueClass(IntWritable.class);
        secondPartJob.setOutputKeyClass(IdentityKey.class);
        secondPartJob.setOutputValueClass(IdentityKeyValue.class);
        
      //  secondPartJob.setInputFormatClass(SequenceFileInputFormat.class);
        
        //Reduce tasks
        secondPartJob.setNumReduceTasks(20);
        
        //Input/Output paths
        FileInputFormat.addInputPath(secondPartJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(secondPartJob, new Path("2SecondPartOutput"));
        System.exit(secondPartJob.waitForCompletion(true) ? 0 : 1);
      
    }
}