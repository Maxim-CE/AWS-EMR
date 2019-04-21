package EMR_DSPS.mainPack;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
 
public class ThirdPart {
		
    public static class MapperClass  extends Mapper<IdentityKey,IdentityKeyValue, IdentityKey, IdentityKeyValue> { 	

        public void map(IdentityKey key, IdentityKeyValue value, Context context) throws IOException,  InterruptedException {
        	context.write(key, value);
        }
    }
    
    public static class PartitionerClass extends Partitioner<IdentityKey,IdentityKeyValue> {
      
        public int getPartition(IdentityKey key, IdentityKeyValue value, int numPartitions) {
        	return Math.abs(key.getFirstKey().getDecade().hashCode()) % numPartitions; 
        }  
    }
    
 
    public static class ReducerClass  extends Reducer<IdentityKey,IdentityKeyValue, SecondKey, Text> {
        private int c2, n;
        
    	public void setup(Context context) {
    		c2 = 0;
    		n = 0;
    	}
    	
        public void reduce(IdentityKey key, Iterable<IdentityKeyValue> values, Context context) throws IOException, InterruptedException {
        	FirstKey key1 = key.getFirstKey();
        //	System.out.println(String.format("Tag: %s, w1: %s, year: %d" ,key.getTag().toString(), key1.getW(), key1.getYear().get()));
        	Iterator<IdentityKeyValue> value = values.iterator();
			while(value.hasNext()) {
        		if (key.getTag().toString().equals("1TotalWords")) 
        			n = value.next().getC1().get();
        		else if (key.getTag().toString().equals("2SingleDecadeWord")) 
        			c2 = value.next().getC1().get();
        		else if (key.getTag().toString().equals("3Collocation")) {
        			IdentityKeyValue val = value.next();
        			HoodContainer biCo = new HoodContainer(
        					val.getW1(), 
        					key1.getWord(), 
        					key1.getDecade(), 
        					val.getC1(), 
        					new IntWritable(c2), 
        					val.getC12());
        			context.write(
        					new SecondKey(
        							getRatio(biCo, n), 
        							key1.getDecade()), 
        					new Text(val.getW1().toString()+" "+key1.getWord().toString()));
        		}
        	}
        }
        private DoubleWritable getRatio(HoodContainer key, int n)  {
        	double hoodRatio = -2*(homieFromTheHoodRatio(
        			key.getC1().get(),
        			key.getC2().get(), 
        			key.getC12().get(),
        			n));
            return new DoubleWritable(hoodRatio);
        }
        private double homieFromTheHoodRatio(int c1, int c2, int c12, int N) {
            double p1 = (double) c12 / c1;
            double p2 = (double) (c2 - c12) / (N - c1);
            double p = (double) c2 / N;
            double logLambda = 	logL(c12, c1, p) + 
            					logL(c2-c12, N-c1, p)  - 
            					logL(c2-c12, N-c1, p2) - 
            					logL(c12, c1, p1);
            return logLambda;
        }

        private double logL(int k, long n, double x) {
        	if (x == 1.0) x = 0.99;
            if (x == 0.0) x = 0.01;
            return 	k * Math.log(x) + 
            		(n-k) * Math.log(1-x);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "ThirdPartJob");
        job.setJarByClass(ThirdPart.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(IdentityKey.class);
        job.setMapOutputValueClass(IdentityKeyValue.class);
        job.setOutputKeyClass(SecondKey.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(20);
        MultipleInputs.addInputPath(job, new Path("1FirstPartOutput"), FirstToThirdFormat.class,ThirdPart.MapperClass.class);
        MultipleInputs.addInputPath(job, new Path("2SecondPartOutput"), SecondToThirdFormat.class,ThirdPart.MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path("3ThirdPartOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      
    }
}