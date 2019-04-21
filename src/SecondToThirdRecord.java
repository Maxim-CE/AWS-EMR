package EMR_DSPS.mainPack;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SecondToThirdRecord extends RecordReader<IdentityKey,IdentityKeyValue> { 
	IdentityKeyValue identityKeyVal;
    LineRecordReader reader;
	IdentityKey identityKey;
	
    public SecondToThirdRecord() {
        reader = new LineRecordReader(); 
    }
    
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }
 
    public void close() throws IOException {
        reader.close();        
    }
    
    public boolean nextKeyValue() throws IOException, InterruptedException {
    	if (!reader.nextKeyValue()) {
            return false;
        }
        String line = reader.getCurrentValue().toString();
        String[] keyValue = line.split("\t");
        String[] keyFields = keyValue[0].split(" ");
        String[] valueFields = keyValue[1].split(" ");
        
        try {
        	identityKey = new IdentityKey(
        			new FirstKey(new Text(keyFields[0]),
        			new IntWritable(Integer.parseInt(keyFields[1]))),
        			new Text(keyFields[2]));
        	identityKeyVal = new IdentityKeyValue(
        			new Text(valueFields[0]), 
        			new IntWritable(Integer.parseInt(valueFields[1])), 
        			new IntWritable(Integer.parseInt(valueFields[2])));
        } catch (ArrayIndexOutOfBoundsException e) {
        	System.out.println("\n"+ e.getMessage());
            for (String keyss : keyFields)
            	System.out.print(" "+ keyss);
            System.out.println("\n"+ e.getMessage());
            for (String valss : valueFields)
            	System.out.print(" "+ valss);
            System.out.println("\n"+ e.getMessage());

        }
        return true;
    }
    
    public IdentityKey getCurrentKey() throws IOException, InterruptedException {
    	return identityKey;
    }
    
    public IdentityKeyValue getCurrentValue() throws IOException, InterruptedException {
        return identityKeyVal;
    }    
 
     public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
 
}