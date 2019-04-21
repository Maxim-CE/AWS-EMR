package EMR_DSPS.mainPack;

import java.util.Iterator;
import java.util.HashSet;

public class StopWords {
	protected HashSet<String> stpWords;
	
	public StopWords() {
		stpWords = new HashSet<>();
	}
	
    public boolean contains(String word) {
        return stpWords.contains(word);
    }
    
    public int size() {
        return stpWords.size();
    }
    
    public Iterator<String> iterator() {
        return stpWords.iterator();
    }
}
