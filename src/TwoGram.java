package EMR_DSPS.mainPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TwoGram implements WritableComparable<TwoGram> {
	private Text w1, w2;
    private IntWritable year;
    
    TwoGram() {
        this.w1 = new Text();
        this.w2 = new Text();
        this.year = new IntWritable(); 
    }
    
    TwoGram(Text w1, Text w2, IntWritable year) {
        this.w1 = w1;
        this.w2 = w2;
        this.year = year;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	w1.readFields(in);
        w2.readFields(in);
        year.readFields(in);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        w1.write(out);
        w2.write(out);
        year.write(out);
    }
    @Override
    public int compareTo(TwoGram other) {
        int ret = year.compareTo(other.year);
        if (ret == 0)
        	ret = w1.compareTo(other.w1);
        if (ret == 0)
        	ret = w2.compareTo(other.w2);
        return ret;
    }
    
    public IntWritable getYear() {
    	return year;
    }
    
    public Text getW2() {
    	return w2;
    }
    
    public Text getW1() {
    	return w1;
    }
    
    @Override
    public boolean equals(Object o) {
    	if (o instanceof TwoGram) {
    		TwoGram other = (TwoGram) o;
    		return w1.equals(other.w1) && w2.equals(other.w2) && year.equals(other.year);
    	}
    	return false;
    
    }
}