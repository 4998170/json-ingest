package us.yuxin.ingest.accumulo;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Splitter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import us.yuxin.ingest.Ingest;
import us.yuxin.sa.transformer.JsonDecomposer;
import us.yuxin.sa.transformer.Transformer;
import us.yuxin.sa.transformer.TransformerFactory;

public class AIngestMapper implements Mapper<LongWritable, Text, NullWritable, NullWritable> {
  protected JsonDecomposer decomposer;
  protected MessagePack messagePack;

  protected ColumnVisibility columnVisbility;

  protected ZooKeeperInstance instance;
  protected Connector connector;
  protected BatchWriter writer;

  protected boolean storeAttirbute;
  protected boolean storeMsgpack;

  JobConf job;


  protected void createConnection()
    throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    String connectionToken = job.get(AIngest.CONF_ACCULUMO_CONNECTION_TOKEN);

    Iterator<String> tokens = Splitter.on(':').split(connectionToken).iterator();

    String instanceName = tokens.next();
    String zooKeepers = tokens.next();
    String user = tokens.next();
    String password = tokens.next();
    String visibility = tokens.next();
    String tableName = tokens.next();

    instance = new ZooKeeperInstance(instanceName, zooKeepers);
    connector = instance.getConnector(user, password.getBytes());
    writer = connector.createBatchWriter(tableName,
      job.getInt(AIngest.CONF_ACCULUMO_MAX_MEMORY, AIngest.ACCUMULO_MAX_MEMORY),
      job.getInt(AIngest.CONF_ACCUMULO_MAX_LATENCY, AIngest.ACCUMULO_MAX_LATENCY),
      job.getInt(AIngest.CONF_ACCUMULO_MAX_WRITE_THREADS, AIngest.ACCUMULO_MAX_WRITE_THREADS));

    columnVisbility = new ColumnVisibility(visibility);
  }


  protected void closeConnection() {
    if (writer != null) {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        // TODO: logger.warn("Unable to shutdown Accumulo Batch Writer", e);
      }
      writer = null;
    }

    connector = null;
    instance = null;
  }


  @Override
  public void configure(JobConf conf) {
    this.job = conf;

    storeAttirbute = conf.getBoolean(Ingest.CONF_INGEST_STORE_ATTR, false);
    storeMsgpack = conf.getBoolean(Ingest.CONF_INGEST_STORE_MSGPACK, true);

    if (storeMsgpack)
      messagePack = new MessagePack();

    Transformer transformer = null;
    try {
      transformer = TransformerFactory.get(job.get(Ingest.CONF_INGEST_TRANSFORMER));
    } catch (Exception e) {
      e.printStackTrace();
    }

    decomposer = new JsonDecomposer(transformer);
    decomposer.start();
  }


  @Override
  public void map(LongWritable key, Text value, OutputCollector<NullWritable, NullWritable> nullWritableNullWritableOutputCollector, Reporter reporter) throws IOException {
    // skip blank line.
    if (value.getLength() == 0)
      return;

    byte[] raw = value.getBytes();

    try {
      if (writer == null) {
        createConnection();
      }

      Map<String, Object> msg = decomposer.readValue(raw);
      Text rowId = new Text(decomposer.getKey());

      // System.out.println("rowId:" + rowId.toString());
      if (rowId == null) {
        // TODO ... Error Handler
        return;
      }

      Mutation mutation = new Mutation(rowId);
      mutation.put(new Text("raw"), new Text(value), columnVisbility, new Value(new byte[0]));


      if (storeMsgpack) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Packer packer = messagePack.createPacker(bos);
        try {
          packer.write(msg);
          mutation.put(new Text("mp"), new Text(bos.toByteArray()), columnVisbility, new Value(new byte[0]));
          bos.close();
        } catch (IOException e) {
          // TODO ... Error Handler
        }
      }


      if (storeAttirbute) {
        for (String k : msg.keySet()) {
          if (k.startsWith("__"))
            continue;

          Object v = msg.get(k);

          if (v == null)
            continue;

          if (v.equals(""))
            continue;

          mutation.put(new Text("a"), new Text(k.toLowerCase()),
            columnVisbility, new Value(v.toString().getBytes()));
        }
      }


      writer.addMutation(mutation);
      // writer.flush();
      // System.out.println("Write OK:" + rowId.toString());
    } catch (Exception e) {
      e.printStackTrace();
      // TODO ... Error Handler
    }
  }


  @Override
  public void close() throws IOException {
    closeConnection();
  }
}
