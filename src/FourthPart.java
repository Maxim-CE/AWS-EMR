package EMR_DSPS.mainPack;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FourthPart {
	
    public static class MapperClass  extends Mapper<SecondKey,Text, SecondKey, Text> { 	
    	
        public void map(SecondKey secondKey, Text value, Context context) throws IOException,  InterruptedException {
        	context.write(secondKey, value);
        }      
    }
    
    public static class PartitionerClass extends Partitioner<SecondKey, Text> {     
        public int getPartition(SecondKey secondKey, Text value,int numberOfPartitions) {        	
        	return (Math.abs(secondKey.getDecade().toString().hashCode())) % numberOfPartitions; 
        }  
    }
    
    public static class ReducerClass  extends Reducer<SecondKey,Text, Text, DoubleWritable> {
        public void reduce(SecondKey secondKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	int top100Counter = 0;
        	for (Text value : values) {
        		if (top100Counter < 100) {
        			context.write(	new Text(	value.toString() + 
        										"\t" + 
        										secondKey.getDecade().toString()), 
        									 	secondKey.getRatio());
        		}
        		top100Counter ++ ;
        	}
        }
    }

    public static void main(String[] args) throws Exception {
        
    	//configuration
    	Configuration conf = new Configuration();
    	
        //set MapReduce 
        Job fourthPartJob = new Job(conf, "FourthPartJob");
        fourthPartJob.setJarByClass(FourthPart.class);
        fourthPartJob.setMapperClass(MapperClass.class);
        fourthPartJob.setPartitionerClass(PartitionerClass.class);
      //fourthPartJob.setCombinerClass(ReducerClass.class);
        fourthPartJob.setGroupingComparatorClass(FourthPartGroupingComperator.class);

        fourthPartJob.setReducerClass(ReducerClass.class);
        fourthPartJob.setMapOutputKeyClass(SecondKey.class);
        fourthPartJob.setMapOutputValueClass(Text.class);
        fourthPartJob.setOutputKeyClass(Text.class);
        fourthPartJob.setOutputValueClass(DoubleWritable.class);
        
        //Reduce tasks
        int numOfTasks = conf.getInt("numOfDecades",20);
        fourthPartJob.setNumReduceTasks(numOfTasks);
        
        //Inout format
        fourthPartJob.setInputFormatClass(ThirdToFourthInputFormat.class);
        
        //Input/Output paths
        FileInputFormat.addInputPath(fourthPartJob, new Path("3ThirdPartOutput"));
        FileOutputFormat.setOutputPath(fourthPartJob, new Path(args[0]));
        
        System.exit(fourthPartJob.waitForCompletion(true) ? 0 : 1);
      
    }
}