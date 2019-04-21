package EMR_DSPS.mainPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FirstKey implements WritableComparable<FirstKey> {
	private Text word;
    private IntWritable decade;

    FirstKey() {
        this.word = new Text(); 
        this.decade = new IntWritable();
    }
    
    FirstKey(Text word, IntWritable decade) {
        this.word = word;
        this.decade = decade;
    }
    
    public void readFields(DataInput in) throws IOException {
    	word.readFields(in);
        decade.readFields(in);
    }
    
    public int hashCode() {
        return word.hashCode() + decade.hashCode();
    }
    
    public void write(DataOutput out) throws IOException {
        word.write(out);
        decade.write(out);
    }

    public IntWritable getDecade() {
    	return decade;
    }
    public Text getWord() {
    	return word;
    }
    
    public boolean equals(Object o) {
    	if (o instanceof FirstKey) {
    		FirstKey other = (FirstKey) o;
    		return word.equals(other.word)  && decade.equals(other.decade);
    	}
    	return false;  
    }
    
    public int compareTo(FirstKey other) {
        int ret = word.compareTo(other.word);
        if (ret != 0 && word.toString().equals("$"))
			return -1;
		if (ret != 0 && other.word.toString().equals("$"))
			return 1;
        if (ret == 0)
        	ret = decade.compareTo(other.decade);
        return ret;
    }
    
    public int compDecade(FirstKey other ) {
        return decade.compareTo(other.decade);
    }
    
    public int compWord(FirstKey other ) {
        return word.compareTo(other.word);
    }
    
    public String toString() {
        return word.toString() + " " + decade.toString();
    }
}