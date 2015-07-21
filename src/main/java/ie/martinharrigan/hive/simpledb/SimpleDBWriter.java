package ie.martinharrigan.hive.simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.amazonaws.services.simpledb.*;
import com.amazonaws.services.simpledb.model.*;

public class SimpleDBWriter implements RecordWriter {
  SimpleDBTable table;

  public SimpleDBWriter(String accessKeyId, String secretAccessKey) {
    table = new SimpleDBTable(accessKeyId, secretAccessKey);
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (table != null)
      table.close();
  }

  @Override
  public void write(Writable w) throws IOException {
    MapWritable map = (MapWritable) w;
    String domain = null, id = null, key = null, value = null;
    List<ReplaceableAttribute> replaceableAttributes =
      new ArrayList<ReplaceableAttribute>();
    for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
      String k = entry.getKey().toString();
      String v = entry.getValue().toString();
      if (k.equals("domain")) {
        domain = v;
      } else if (k.equals("id")) {
        id = v;
      } else if (k.equals("key")) {
        key = v;
      } else if (k.equals("value")) {
        value = v;
      } else {
        replaceableAttributes.add(new ReplaceableAttribute(k, v, true));
      }
    }
    if (key != null && value != null) {
      replaceableAttributes.add(new ReplaceableAttribute(key, value, true));
    }
    if (domain != null && id != null && replaceableAttributes.size() > 0) {
      table.save(domain, id, replaceableAttributes);
    }
  }

  private Object getObjectFromWritable(Writable w) {
    if (w instanceof IntWritable) {
      return ((IntWritable) w).get();
    } else if (w instanceof ShortWritable) {
      return ((ShortWritable) w).get();
    } else if (w instanceof ByteWritable) {
      return ((ByteWritable) w).get();
    } else if (w instanceof BooleanWritable) {
      return ((BooleanWritable) w).get();
    } else if (w instanceof LongWritable) {
      return ((LongWritable) w).get();
    } else if (w instanceof FloatWritable) {
      return ((FloatWritable) w).get();
    } else if (w instanceof DoubleWritable) {
      return ((DoubleWritable) w).get();
    } else if (w instanceof NullWritable) {
      return null;
    } else {
      return w.toString();
    }
  }
}
