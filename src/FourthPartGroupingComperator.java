package EMR_DSPS.mainPack;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FourthPartGroupingComperator extends WritableComparator  {  	
    	public FourthPartGroupingComperator() {
    		super(SecondKey.class, true);
    	}
		public int compare(WritableComparable other1, WritableComparable other2) {
    		SecondKey secondKey1 = (SecondKey) other1;
    		SecondKey secondKey2 = (SecondKey) other2;    		
			return secondKey1.getDecade().compareTo(secondKey2.getDecade());
		}
    }