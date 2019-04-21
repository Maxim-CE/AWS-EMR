package EMR_DSPS.mainPack;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class FirstToThirdRecord extends RecordReader<IdentityKey,IdentityKeyValue> { 
	IdentityKeyValue identityKeyVal;
    LineRecordReader reader;
	IdentityKey identityKey;

    public FirstToThirdRecord() {
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

        identityKey = new IdentityKey(
        		new FirstKey(new Text(keyFields[0]),
        		new IntWritable(Integer.parseInt(keyFields[1]))),
        		new Text(keyFields[2]));
        identityKeyVal = new IdentityKeyValue(Integer.parseInt(valueFields[1]));
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