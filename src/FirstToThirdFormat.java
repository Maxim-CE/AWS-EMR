package EMR_DSPS.mainPack;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
public class FirstToThirdFormat extends FileInputFormat<IdentityKey, IdentityKeyValue> {
      public RecordReader<IdentityKey, IdentityKeyValue> createRecordReader(InputSplit split,TaskAttemptContext context) {
        return new FirstToThirdRecord();
      }
      
      protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
      }
 
}