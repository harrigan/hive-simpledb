package ie.martinharrigan.hive.simpledb;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;

public class ConfigurationUtil {
  public static final String ACCESS_KEY_ID = "simpledb.access_key_id";
  public static final String SECRET_ACCESS_KEY = "simpledb.secret_access_key";
  public static final String COLUMN_MAPPING = "simpledb.column.mapping";

  public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(
    ACCESS_KEY_ID, SECRET_ACCESS_KEY, COLUMN_MAPPING);

  public final static String getAccessKeyId(Configuration conf) {
    return conf.get(ACCESS_KEY_ID);
  }

  public final static String getSecretAccessKey(Configuration conf) {
    return conf.get(SECRET_ACCESS_KEY);
  }

  public final static String getColumnMapping(Configuration conf) {
    return conf.get(COLUMN_MAPPING);
  }

  public static void copySimpleDBProperties(Properties from,
      Map<String, String> to) {
    for (String key : ALL_PROPERTIES) {
      String value = from.getProperty(key);
      if (value != null) {
        to.put(key, value);
      }
    }
  }

  public static String[] getAllColumns(String columnMappingString) {
    return columnMappingString.split(",");
  }
}
