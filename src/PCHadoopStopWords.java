package EMR_DSPS.mainPack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PCHadoopStopWords extends StopWords{
	
    public PCHadoopStopWords(String stopWordLoc) {
    	super();
        try {
        	Configuration conf = new Configuration();
        	Path path = new Path(stopWordLoc);
        	FileSystem fs = FileSystem.get(path.toUri(), conf);
        	InputStream is = fs.open(path);
    	    BufferedReader br=new BufferedReader(new InputStreamReader(is));	
            String line = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    stpWords.add(line);
                }
            }
            is.close();
            br.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
    }

    
}