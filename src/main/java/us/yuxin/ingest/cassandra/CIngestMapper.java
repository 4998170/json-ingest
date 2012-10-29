package us.yuxin.ingest.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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
import org.msgpack.MessagePack;
import us.yuxin.ingest.Ingest;
import us.yuxin.sa.transformer.JsonDecomposer;
import us.yuxin.sa.transformer.Transformer;
import us.yuxin.sa.transformer.TransformerFactory;


public class CIngestMapper implements Mapper<LongWritable, Text, NullWritable, NullWritable> {
  JobConf job;
  protected boolean storeAttirbute;
  JsonDecomposer decomposer;

  MutationBatch mb;

  protected MessagePack messagePack;

  AstyanaxContext<Keyspace> context;
  Keyspace ks;
  ColumnFamily<String, String> cf;


  @Override
  public void configure(JobConf job) {
    this.job = job;

    messagePack = new MessagePack();
    storeAttirbute = job.getBoolean(Ingest.CONF_INGEST_STORE_ATTR, false);

    Transformer transformer = null;
    try {
      transformer = TransformerFactory.get((String) job.get(Ingest.CONF_INGEST_TRANSFORMER));
    } catch (Exception e) {
      e.printStackTrace();
    }

    decomposer = new JsonDecomposer(transformer);
    decomposer.start();

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

    Map<String, Object> msg = decomposer.readValue(raw);
    String rowId = new String(decomposer.getRowKey());

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
    decomposer.stop();
    context.shutdown();
  }
}
