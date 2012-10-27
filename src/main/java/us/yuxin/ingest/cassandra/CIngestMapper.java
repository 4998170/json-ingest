package us.yuxin.ingest.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Splitter;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.msgpack.MessagePack;


public class CIngestMapper implements Mapper<LongWritable, Text, NullWritable, NullWritable> {
  JobConf job;
  protected boolean storeAttirbute;

  MutationBatch mb;
  protected ObjectMapper mapper;
  protected MessagePack messagePack;

  protected DateTimeFormatter timeFmt;
  protected AtomicInteger serial;
  protected AtomicInteger oidSerial;

  AstyanaxContext<Keyspace> context;
  Keyspace ks;
  ColumnFamily<String, String> cf;

  @Override
  public void configure(JobConf job) {
    this.job = job;

    mapper = new ObjectMapper();
    messagePack = new MessagePack();

    timeFmt = DateTimeFormat.forPattern("YYYYMMddHHmmssSSS");
    oidSerial = new AtomicInteger(0);
    serial = new AtomicInteger(0);

    storeAttirbute = job.getBoolean(CIngest.CONF_INGEST_STORE_ATTR, false);

    Iterator<String> tokens = Splitter.on("///").split(job.get(CIngest.CONF_CASSANDRA_CONNECT_TOKEN)).iterator();

    String clusterName = tokens.next();
    String seeds = tokens.next();
    String keyspaceName = tokens.next();
    String columeFamilyName = tokens.next();

    context = new AstyanaxContext.Builder()
      .forCluster(clusterName).forKeyspace(keyspaceName)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.TOKEN_AWARE))
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("cp")
        .setPort(9160)
        .setMaxConnsPerHost(2)
        .setSeeds(seeds))
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    ks = context.getEntity();

    cf = new ColumnFamily<String, String>(columeFamilyName,
      StringSerializer.get(), StringSerializer.get());
  }


  @Override
  public void map(LongWritable key, Text value,
                  OutputCollector<NullWritable, NullWritable> collector,
                  Reporter reporter) throws IOException {
    if (value.getLength() == 0)
      return;

    byte[] raw = value.getBytes();


    Map<String, Object> msg = mapper.readValue(raw, Map.class);
    String rowId = createRowId(msg);

    // System.out.println("rowId:" + rowId.toString());
    if (rowId == null) {
      // TODO ... Error Handler
      return;
    }

    if (mb == null) {
      mb = ks.prepareMutationBatch();
    }
    ColumnListMutation<String> c = mb.withRow(cf, rowId);
    c.putColumn("raw", value.toString(), null);

    if (storeAttirbute) {
      for (String k : msg.keySet()) {
        if (k.startsWith("__"))
          continue;

        Object v = msg.get(k);

        if (v == null)
          continue;

        if (v.equals(""))
          continue;

        c.putColumn(k.toLowerCase(), v.toString(), null);
      }
    }

    try {
      if (mb.getRowCount() > 300) {
        OperationResult<Void> result = mb.execute();
        mb = null;
      }
    } catch (ConnectionException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      mb = null;
    }
  }



  @Override
  public void close() throws IOException {
    if (mb != null && ! mb.isEmpty()) {
      try {
        OperationResult<Void> result = mb.execute();
      } catch (ConnectionException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    context.shutdown();
  }


  protected String createRowId(Map<String, Object> data) {
    Long ots = null;
    Object ov;

    ov = data.get("logTimestamp");
    if (ov == null) {
      //return new Text("9999");
      return null;
    }

    if (ov instanceof Long) {
      ots = (Long) ov;
    } else if (ov instanceof Integer) {
      // logger.warn("Invalid logTimestamp(int) - raw-log:" + new String(event.getBody()));
      ots = ((Integer) ov).longValue();
    } else if (ov instanceof String) {
      // logger.warn("Invalid logTimestamp(string) - raw-log:" + new String(event.getBody()));
      ots = Long.parseLong((String) ov);
    }

    if (ots == null) {
      // logger.warn("Invalid logTimestamp(empty) - raw-log:" + new String(event.getBody()));
      // return new Text("9999");
      return null;
    }

    Integer oid = (Integer) data.get("oid");
    if (oid == null)
      oid = oidSerial.incrementAndGet();

    Integer ser = serial.incrementAndGet();
    return String.format("%s.%04d%04d",
      timeFmt.print(ots), oid % 10000, ser % 10000);
  }
}
