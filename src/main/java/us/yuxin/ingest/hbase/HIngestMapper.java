package us.yuxin.ingest.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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


public class HIngestMapper implements Mapper<LongWritable, Text, NullWritable, NullWritable> {

  JsonDecomposer decomposer;
  protected MessagePack messagePack;

  protected boolean storeAttirbute;
  protected boolean storeMsgpack;

  protected JobConf job;
  protected Configuration hbase;
  protected HTable hTable;


  protected void createConnection() throws IOException {

    String connectToken = job.get(HIngest.CONF_HBASE_CONNECT_TOKEN);
    Iterator<String> tokens = Splitter.on("///").split(connectToken).iterator();

    String zooKeepers = tokens.next();
    String tableName = tokens.next();

    hbase = HBaseConfiguration.create();

    if (zooKeepers.contains(":")) {
      int off = zooKeepers.indexOf(":");
      hbase.set("hbase.zookeeper.quorum", zooKeepers.substring(0, off));
      hbase.set("hbase.zookeeper.property.clientPort", zooKeepers.substring(off + 1));
    } else {
      hbase.set("hbase.zookeeper.quorum", zooKeepers);
    }


    hTable = new HTable(hbase, tableName);
    hTable.setAutoFlush(false);
  }


  protected void closeConnection() {
    hTable = null;
    if (hTable != null) {
      try {
        if (hTable != null) {
          hTable.flushCommits();
          hTable.close();
        }
      } catch (IOException e) {
        // TODO logger
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      hTable = null;
    }
    hbase = null;
  }


  @Override
  public void configure(JobConf conf) {
    this.job = conf;
    storeAttirbute = conf.getBoolean(Ingest.CONF_INGEST_STORE_ATTR, false);
    storeMsgpack = conf.getBoolean(Ingest.CONF_INGEST_STORE_MSGPACK, true);

    Transformer transformer = null;
    try {
      transformer = TransformerFactory.get((String) job.get(Ingest.CONF_INGEST_TRANSFORMER));
    } catch (Exception e) {
      e.printStackTrace();
    }

    decomposer = new JsonDecomposer(transformer);
    decomposer.start();

    if (storeMsgpack) {
      messagePack = new MessagePack();
    }


    try {
      createConnection();
    } catch (IOException e) {
      e.printStackTrace();
      // TODO Error handler
    }
  }


  @Override
  public void map(LongWritable key, Text value, OutputCollector<NullWritable, NullWritable> nullWritableNullWritableOutputCollector, Reporter reporter) throws IOException {
    // skip blank line.
    if (value.getLength() == 0)
      return;

    String text = value.toString();
    byte[] raw = text.getBytes();

    // System.out.println("VALUE:" + value + ", bytes[]:" + raw + ", length:" + raw.length);
    try {
      Map<String, Object> msg = decomposer.readValue(raw);

      byte[] rowId = decomposer.getRowKey();
      if (rowId == null) {
        // TODO ... Error Handler
        return;
      }

      Put put = new Put(rowId);
      put.setWriteToWAL(false);

      put.add("raw".getBytes(), new byte[0], raw);

      if (storeMsgpack) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Packer packer = messagePack.createPacker(bos);
        try {
          packer.write(msg);
          put.add("mp".getBytes(), new byte[0], bos.toByteArray());
          bos.close();
        } catch (IOException e) {
          e.printStackTrace();
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

          put.add("a".getBytes(), k.toLowerCase().getBytes(), v.toString().getBytes());
        }
        hTable.put(put);
      }
    } catch (Exception e) {
      e.printStackTrace();
      // TODO ... Error Handler
    }
  }


  @Override
  public void close() throws IOException {
    decomposer.stop();
    closeConnection();
  }
}
