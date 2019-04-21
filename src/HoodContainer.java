package EMR_DSPS.mainPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class HoodContainer implements WritableComparable<HoodContainer> {
    private IntWritable year;
    private IntWritable c1, c2, c12;
	private Text w1, w2;

    
    HoodContainer() {
        this.c1 = new IntWritable(); 
        this.c2 = new IntWritable();
        this.c12 = new IntWritable();
        this.w1 = new Text();
        this.w2 = new Text();
        this.year = new IntWritable();
    }
    
    HoodContainer(Text w1, Text w2, IntWritable year, IntWritable c1, IntWritable c2, IntWritable c12) {
        this.c1 = c1;
        this.c2 = c2;
        this.c12 = c12;
        this.w1 = w1;
        this.w2 = w2;
        this.year = year;
    }
    HoodContainer(TwoGram bigram, IntWritable c1, IntWritable c2, IntWritable c12) {
        this.c1 = c1;
        this.c2 = c2;
        this.c12 = c12;
    	this.w1 = bigram.getW1();
        this.w2 = bigram.getW2();
        this.year = bigram.getYear();
    }
    
    public void readFields(DataInput in) throws IOException {
        c1.readFields(in);
        c2.readFields(in);
        c12.readFields(in);
    	w1.readFields(in);
        w2.readFields(in);
        year.readFields(in);
    }
    
    public void write(DataOutput out) throws IOException {
        w1.write(out);
        w2.write(out);
        year.write(out);
        c1.write(out);
        c2.write(out);
        c12.write(out);
    }

    public int compareTo(HoodContainer other) {
        int ret = year.compareTo(other.year);
        if (ret == 0)
        	ret = w1.compareTo(other.w1);
        if (ret == 0)
        	ret = w2.compareTo(other.w2);
        if (ret == 0)
        	ret = c1.compareTo(other.c1);
        if (ret == 0)
        	ret = c2.compareTo(other.c2);
        if (ret == 0)
        	ret = c12.compareTo(other.c12);
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
    
    public IntWritable getC1() {
    	return c1;
    }
    
    public IntWritable getC2() {
    	return c2;
    }
    
    public IntWritable getC12() {
    	return c12;
    }
    public Text toText() {
    	return new Text(w1+" "+w2+" "+year);
    }
    
    public boolean equals(Object o) {
    	if (o instanceof HoodContainer) {
    		HoodContainer other = (HoodContainer) o;
    		return w1.equals(other.w1) && w2.equals(other.w2) && year.equals(other.year) &&
    				c1.equals(other.c1) && c2.equals(other.c2) && c12.equals(other.c12);
    	}
    	return false;
    
    }
}