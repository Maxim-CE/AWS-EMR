package EMR_DSPS.mainPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IdentityKeyValue implements Writable{
	private Text w1;
    private IntWritable c12, c1;
    
    IdentityKeyValue() {
        this.w1 = new Text();
        this.c1 = new IntWritable();
        this.c12 = new IntWritable();
    }
    
    IdentityKeyValue(Text w1, IntWritable c1,IntWritable c12) {
        this.w1 = w1;
        this.c1 = c1;
        this.c12 = c12;
    } 
    
    IdentityKeyValue(IntWritable c1) {
        this.w1 = new Text("0");
        this.c1 = c1;
        this.c12 = new IntWritable(0);
    }
    
    IdentityKeyValue(int c1) {
        this.w1 = new Text("0");
        this.c1 = new IntWritable(c1);
        this.c12 = new IntWritable(0);
    }
    
    public IntWritable getC12() {
    	return c12;
    }
    public IntWritable getC1() {
    	return c1;
    }
    
    public Text getW1() {
    	return w1;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	w1.readFields(in);
        c1.readFields(in);
    	c12.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        w1.write(out);
        c1.write(out);
        c12.write(out);
    }
    
    @Override
    public String toString() {
        return w1.toString() + " " + c1.toString() + " " + c12.toString();
    }
}
