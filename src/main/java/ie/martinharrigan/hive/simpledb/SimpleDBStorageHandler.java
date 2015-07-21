package ie.martinharrigan.hive.simpledb;

import static ie.martinharrigan.hive.simpledb.ConfigurationUtil.ACCESS_KEY_ID;
import static ie.martinharrigan.hive.simpledb.ConfigurationUtil.SECRET_ACCESS_KEY;
import static ie.martinharrigan.hive.simpledb.ConfigurationUtil.copySimpleDBProperties;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class SimpleDBStorageHandler implements HiveStorageHandler {
  private Configuration mConf = null;

  public SimpleDBStorageHandler() {
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    copySimpleDBProperties(properties, jobProperties);
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return new DummyMetaHook();
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DummyInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return SimpleDBOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return SimpleDBSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return mConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.mConf = conf;
  }

  private class DummyMetaHook implements HiveMetaHook {

    @Override
    public void commitCreateTable(Table tbl) throws MetaException {
    }

    @Override
    public void commitDropTable(Table tbl, boolean deleteData)
        throws MetaException {
      boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
      if (deleteData && isExternal) {
      } else if (deleteData && !isExternal) {
        String accessKeyId = tbl.getParameters().get(ACCESS_KEY_ID);
        String secretAccessKey = tbl.getParameters().get(SECRET_ACCESS_KEY);
        SimpleDBTable table = new SimpleDBTable(accessKeyId, secretAccessKey);
        table.close();
      }
    }

    @Override
    public void preCreateTable(Table tbl) throws MetaException {
    }

    @Override
    public void preDropTable(Table tbl) throws MetaException {
    }

    @Override
    public void rollbackCreateTable(Table tbl) throws MetaException {
    }

    @Override
    public void rollbackDropTable(Table tbl) throws MetaException {
    }
  }
}
