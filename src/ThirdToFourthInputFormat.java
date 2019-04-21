package EMR_DSPS.mainPack;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
public class ThirdToFourthInputFormat extends FileInputFormat<SecondKey, Text> {
      @Override
      public RecordReader<SecondKey, Text> createRecordReader(InputSplit split,TaskAttemptContext context) {
        return new ThirdToFourthRecordReader();
      }
      
      @Override
      protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec compressionCodec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return compressionCodec == null;
      }
 
}