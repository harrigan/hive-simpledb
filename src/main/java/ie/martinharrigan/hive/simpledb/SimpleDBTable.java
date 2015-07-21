package ie.martinharrigan.hive.simpledb;

import java.util.List;

import com.amazonaws.auth.*;
import com.amazonaws.services.simpledb.*;
import com.amazonaws.services.simpledb.model.*;

public class SimpleDBTable {
  private AmazonSimpleDB sdb;

  public SimpleDBTable(String accessKeyId, String secretAccessKey) {
    sdb = new AmazonSimpleDBClient(new BasicAWSCredentials(accessKeyId,
      secretAccessKey));
  }

  public void close() {
    sdb = null;
  }

  public void save(String domain, String id, List<ReplaceableAttribute> replaceableAttributes) {
    for (int retry = 1; ; retry++) {
      try {
        sdb.putAttributes(new PutAttributesRequest(domain, id, replaceableAttributes));
        break;
      } catch (RuntimeException e) {
        if (retry >= 2) {
          throw e;
        } else {
          if (!sdb.listDomains().getDomainNames().contains(domain)) {
            sdb.createDomain(new CreateDomainRequest(domain));
          }
        }
      }
    }
  }
}
