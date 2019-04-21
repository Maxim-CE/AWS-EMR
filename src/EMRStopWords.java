package EMR_DSPS.mainPack;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class EMRStopWords extends StopWords{

    public EMRStopWords(String resource) {
        super();
        try {
        	AmazonS3 s3 = AmazonS3ClientBuilder
        			.standard()
        			.withRegion("us-east-1")
        			.build();
        	S3Object object = s3.getObject(new GetObjectRequest("max.eran.map.reduce", resource));
        	InputStream objectData = object.getObjectContent();
    	    BufferedReader buffer = 
    	    		new BufferedReader(new InputStreamReader(objectData));	
            String line = null;
            while ((line = buffer.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    stpWords.add(line);
                }
            }
            objectData.close();
            buffer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}