package EMR_DSPS.mainPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IdentityKey implements WritableComparable<IdentityKey>{
	private FirstKey firstKey;
	private Text identityTag;
	
	public IdentityKey() {
		firstKey= new FirstKey();
		identityTag = new Text();
	}
	public IdentityKey(FirstKey firstKey, Text identityTag) {
		this.firstKey = firstKey; 
		this.identityTag = identityTag;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		firstKey.readFields(in);
        identityTag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		firstKey.write(out);
        identityTag.write(out);
	}
	
	public FirstKey getFirstKey() {
		return firstKey;
	}
	public Text getTag() {
		return identityTag;
	}

	@Override
	public int compareTo(IdentityKey other) {
		//ThirdPart
		if (	identityTag.toString().equals("1TotalWords") || 
				other.identityTag.toString().equals("1TotalWords") ||
				identityTag.toString().equals("2SingleDecadeWord") || 
				other.identityTag.toString().equals("2SingleDecadeWord") ||
				identityTag.toString().equals("3Collocation") || 
				other.identityTag.toString().equals("3Collocation") ) {
			int ret = firstKey.compDecade(other.firstKey);
			if (ret == 0)
				ret = firstKey.compWord(other.firstKey);
			if (ret == 0)
				ret = identityTag.compareTo(other.identityTag);	
			return ret;
		}
		
		//FirstPart
		if (identityTag.toString().equals("TAGTotal") || other.identityTag.toString().equals("TAGTotal")) {
			int ret = identityTag.compareTo(other.identityTag);
			if (ret != 0 && identityTag.toString().equals("TAGTotal"))
				return -1;
			if (ret != 0 && other.identityTag.toString().equals("TAGTotal"))
				return 1;
			return firstKey.compareTo(other.firstKey);
		}	
		if (identityTag.toString().equals("$") || other.identityTag.toString().equals("$")) {
			int ret = identityTag.compareTo(other.identityTag);
			if (ret != 0 && identityTag.toString().equals("$"))
				return -1;
			if (ret != 0 && other.identityTag.toString().equals("$"))
				return 1;
			return firstKey.compareTo(other.firstKey);
		}
		
		//SecondPart
		int ret = firstKey.compareTo(other.firstKey); 
		if (ret == 0 && (identityTag.toString().equals("$$") || other.identityTag.toString().equals("$$"))) {
			if (identityTag.compareTo(other.identityTag) == 0)
				return 0;
			if (identityTag.toString().equals("$$"))
				return -1;
			if (other.identityTag.toString().equals("$$"))
				return 1;
		}
		
		//ThirdPart	
		if (ret == 0)
			ret = identityTag.compareTo(other.identityTag);;		
		return ret;
	}
	
	@Override
    public String toString() {
        return firstKey.toString() + " " + identityTag.toString();
    }
	
	@Override
    public boolean equals(Object o) {
    	if (o instanceof IdentityKey) {
    		IdentityKey other = (IdentityKey) o;
    		return firstKey.equals(other.firstKey) && identityTag.equals(other.identityTag);
    	}
    	return false;  
    }

}
