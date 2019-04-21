package EMR_DSPS.mainPack;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
 
import java.util.LinkedList;
 
import org.apache.hadoop.conf.Configuration;
 
public class LocalMain {
    public static void main(String[] args) {
 
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("PCHadoop", true);
            conf.set("lang", args[2].toLowerCase());
            
            if (args[2].toLowerCase().equals("heb"))
            	conf.set("locationOfStopWords", "C:\\Users\\maxha\\Downloads\\Jaba\\hadoop\\hadoop-2.6.2\\Hadoop-WordCount\\hebrewStopWords.txt");
            else if (args[2].toLowerCase().equals("eng"))
            	conf.set("locationOfStopWords", "C:\\Users\\maxha\\Downloads\\Jaba\\hadoop\\hadoop-2.6.2\\Hadoop-WordCount\\englishStopWords.txt");
            
            FileSystem fs = FileSystem.get(new Configuration());
            if(fs.exists(new Path(args[1]))){
           	   fs.delete(new Path(args[1]),true);
            }
            if(fs.exists(new Path("/user"))){
            	   fs.delete(new Path("/user"),true);
             }
            
            Job job1 = new Job(conf, "FirstPartJob");
            job1.setJarByClass(FirstPart.class);
            job1.setMapperClass(FirstPart.MapperClass.class);
            job1.setReducerClass(FirstPart.ReducerClass.class);
            job1.setCombinerClass(FirstPart.CombinerClass.class);
            job1.setPartitionerClass(FirstPart.PartitionerClass.class);
            job1.setMapOutputKeyClass(IdentityKey.class);
            job1.setMapOutputValueClass(IntWritable.class);
            job1.setOutputKeyClass(IdentityKey.class);
            job1.setOutputValueClass(IdentityKeyValue.class);
            //job1.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path("1FirstPartOutput"));
 
            Job job2 = new Job(conf, "SecondPartOutput");
            job2.setJarByClass(SecondPart.class);
            job2.setMapperClass(SecondPart.MapperClass.class);
            job2.setReducerClass(SecondPart.ReducerClass.class);
            job2.setCombinerClass(SecondPart.CombinerClass.class);
            job2.setPartitionerClass(SecondPart.PartitionerClass.class);
            job2.setMapOutputKeyClass(IdentityKey.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(IdentityKey.class);
            job2.setOutputValueClass(IdentityKeyValue.class);
            //job2.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path("2SecondPartOutput"));
            
            Job job3 = new Job(conf, "ThirdPartJob");
            job3.setJarByClass(ThirdPart.class);
            job3.setMapperClass(ThirdPart.MapperClass.class);
            job3.setReducerClass(ThirdPart.ReducerClass.class);
            job3.setPartitionerClass(ThirdPart.PartitionerClass.class);
            job3.setMapOutputKeyClass(IdentityKey.class);
            job3.setMapOutputValueClass(IdentityKeyValue.class);
            job3.setOutputKeyClass(SecondKey.class);
            job3.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job3, new Path("1FirstPartOutput"), FirstToThirdFormat.class,ThirdPart.MapperClass.class);
            MultipleInputs.addInputPath(job3, new Path("2SecondPartOutput"), SecondToThirdFormat.class,ThirdPart.MapperClass.class);
            FileOutputFormat.setOutputPath(job3, new Path("3ThirdPartOutput"));
            
            Job job4 = new Job(conf, "FourthPartJob");
            job4.setJarByClass(FourthPart.class);
            job4.setMapperClass(FourthPart.MapperClass.class);
            job4.setReducerClass(FourthPart.ReducerClass.class);
            job4.setPartitionerClass(FourthPart.PartitionerClass.class);
            job4.setGroupingComparatorClass(FourthPartGroupingComperator.class);
            job4.setMapOutputKeyClass(SecondKey.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(DoubleWritable.class);
            job4.setInputFormatClass(ThirdToFourthInputFormat.class);
            job4.setNumReduceTasks(conf.getInt("numOfDecades",3));
            FileInputFormat.addInputPath(job4, new Path("3ThirdPartOutput"));
            FileOutputFormat.setOutputPath(job4, new Path(args[1]));
 
            ControlledJob cJob1 = new ControlledJob(job1,new LinkedList<ControlledJob>());
            ControlledJob cJob2 = new ControlledJob(job2,new LinkedList<ControlledJob>());
            ControlledJob cJob3 = new ControlledJob(job3,new LinkedList<ControlledJob>());
            ControlledJob cJob4 = new ControlledJob(job4,new LinkedList<ControlledJob>());
            cJob2.addDependingJob(cJob1);
            cJob3.addDependingJob(cJob2);
            cJob4.addDependingJob(cJob3);
            JobControl jc = new JobControl("JC");
            jc.addJob(cJob1);
            jc.addJob(cJob2);
            jc.addJob(cJob3);
            jc.addJob(cJob4);
          //jc.run();
            Thread runJControl = new Thread(jc);
            runJControl.start();
            while (!jc.allFinished()) {
            	System.out.println("rolling the joint...");
            	Thread.sleep(1000);
            }
            System.exit(1);     
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
}