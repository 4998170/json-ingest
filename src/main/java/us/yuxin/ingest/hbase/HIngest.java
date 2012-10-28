package us.yuxin.ingest.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import us.yuxin.ingest.Ingest;


public class HIngest extends Ingest {
  public final static String CONF_HBASE_CONNECT_TOKEN = "ingest.hbase.token";
  public final static String CONF_HBASE_JAR_PATH = "ingest.hbase.jar.path";

  @Override
  public int run(String[] args) throws Exception {
    prepareClassPath(CONF_HBASE_JAR_PATH, "/is/app/ingest/hbase/lib");

    Configuration conf = getConf();

    JobConf job = new JobConf(conf);
    job.setJobName(String.format("ingest-hbase--%d", System.currentTimeMillis()));
    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);

    job.setJarByClass(HIngestMapper.class);
    job.setMapperClass(HIngestMapper.class);

    job.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    JobClient.runJob(job);
    return 0;
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = prepareConfiguration();
    int res = ToolRunner.run(conf, new HIngest(), args);
    System.exit(res);
  }
}
