package EMR_DSPS.mainPack;

import java.io.IOException;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class ThirdToFourthRecordReader extends RecordReader<SecondKey,Text> { 
	SecondKey secondKey;
	Text val;
    LineRecordReader lineRecordReader;
   
    public ThirdToFourthRecordReader() {
        lineRecordReader = new LineRecordReader(); 
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }
 
 
    @Override
    public void close() throws IOException {
        lineRecordReader.close();        
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
    	if (!lineRecordReader.nextKeyValue()) {
            return false;
        }
        String line = lineRecordReader.getCurrentValue().toString();
        String[] value = line.split("\t");
        String[] fields = value[0].split(" ");
        String[] valFields = value[1].split(" ");
        
        try {
        	secondKey = new SecondKey(Double.parseDouble(fields[0]), Integer.parseInt(fields[1]));
        	val = new Text(valFields[0]+" "+valFields[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
        	System.out.println("\n"+ e.getMessage());
            for (String k : fields)
            	System.out.print(" "+ k);
            System.out.println("\n"+ e.getMessage());
            for (String v : valFields)
            	System.out.print(" "+ v);
            System.out.println("\n"+ e.getMessage());

        }
        return true;
    }
    
    @Override
    public SecondKey getCurrentKey() throws IOException, InterruptedException {
    	return secondKey;
    }
    
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return val;
    }    
 
 
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }
 
}