package ie.martinharrigan.hive.simpledb;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import static ie.martinharrigan.hive.simpledb.ConfigurationUtil.*;

public class SimpleDBOutputFormat implements OutputFormat<NullWritable,Row>,
HiveOutputFormat<NullWritable, Row>{

  @Override
  public RecordWriter getHiveRecordWriter(JobConf conf,
          Path finalOutPath,
          Class<? extends Writable> valueClass,
          boolean isCompressed,
          Properties tableProperties,
          Progressable progress) throws IOException {
    return new SimpleDBWriter(getAccessKeyId(conf), getSecretAccessKey(conf));
  }

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf conf)
      throws IOException {
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Row> getRecordWriter(
      FileSystem fs, JobConf conf, String s, Progressable progress)
      throws IOException {
    throw new RuntimeException("This operation is not supported.");
  }
}
