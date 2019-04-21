package EMR_DSPS.mainPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class SecondKey implements WritableComparable<SecondKey> {
	private DoubleWritable score;
    private IntWritable year;

    SecondKey() {
        this.score = new DoubleWritable(); 
        this.year = new IntWritable();

    }
    
    SecondKey(DoubleWritable score, IntWritable year) {
        this.score = score;
        this.year = year;

    }
    
    SecondKey(double score, int year) {
        this.score = new DoubleWritable(score);
        this.year = new IntWritable(year);

    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	score.readFields(in);
        year.readFields(in);

    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	score.write(out);
        year.write(out);

    }
    @Override
    public int compareTo(SecondKey other) {
        int ret = year.compareTo(other.year);
        if (ret == 0) {
        	if (Double.isNaN(score.get()) && Double.isNaN(other.score.get()))
        		ret = 0;
        	else if (Double.isNaN(score.get()))
        		ret = 1;
        	else if (Double.isNaN(other.score.get()))
        		ret = -1;
        	else 
        		ret = score.compareTo(other.score);
        }
        return ret;
    }
    
    public IntWritable getDecade() {
    	return year;
    }
    public DoubleWritable getRatio() {
    	return score;
    }

    @Override
    public boolean equals(Object o) {
    	if (o instanceof SecondKey) {
    		SecondKey other = (SecondKey) o;
    		return score.equals(other.score)  && year.equals(other.year);
    	}
    	return false;  
    }
    
    @Override
    public String toString() {
    	return score.toString()+" "+year.toString();
    }
}