package ie.martinharrigan.hive.simpledb;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

class DummyInputFormat implements InputFormat<WritableComparable, MapWritable> {

  @Override
  public RecordReader<WritableComparable, MapWritable> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    throw new IOException("This operation is not supported.");
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int number) throws IOException {
    throw new IOException("This operation is not supported.");
  }
}
