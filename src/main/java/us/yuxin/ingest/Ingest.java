package us.yuxin.ingest;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public abstract class Ingest extends Configured implements Tool {
  public final static String CONF_INGEST_STORE_ATTR = "ingest.store.attr";
  public final static String CONF_INGEST_STORE_MSGPACK = "ingest.store.msgpack";
  public final static String CONF_INGEST_TRANSFORMER = "ingest.transformer";


  protected static Configuration prepareConfiguration() {
    Configuration conf = new Configuration();

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Ingest.class.getClassLoader();
    }

    URL defaultURL = classLoader.getResource("ingest-default.xml");
    if (defaultURL != null)
      conf.addResource(defaultURL);

    URL siteURL = classLoader.getResource("ingest-site.xml");
    if (siteURL != null)
      conf.addResource(siteURL);

    return conf;
  }


  protected void prepareClassPath(
    String name, String defaultValue) throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(conf.get(name, defaultValue)));
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.getPath().toString().endsWith(".jar")) {
        DistributedCache.addFileToClassPath(fileStatus.getPath(), conf, fs);
      }
    }
    fs.close();
  }
}
