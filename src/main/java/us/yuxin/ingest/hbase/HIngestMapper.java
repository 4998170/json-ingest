package us.yuxin.ingest.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;


public class HIngestMapper implements Mapper<LongWritable, Text, NullWritable, NullWritable> {
  ObjectMapper mapper;
  protected MessagePack messagePack;

  protected DateTimeFormatter timeFmt;

  protected AtomicInteger serial;
  protected AtomicInteger oidSerial;

  protected boolean storeAttirbute;

  protected JobConf job;
  protected Configuration hbase;
  protected HTable hTable;

  protected byte[] createRowId(Map<String, Object> data) {
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
      timeFmt.print(ots), oid % 10000, ser % 10000).getBytes();
  }


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

    mapper = new ObjectMapper();
    messagePack = new MessagePack();

    timeFmt = DateTimeFormat.forPattern("YYYYMMddHHmmssSSS");
    oidSerial = new AtomicInteger(0);
    serial = new AtomicInteger(0);

    storeAttirbute = conf.getBoolean(HIngest.CONF_INGEST_STORE_ATTR, false);

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
      Map<String, Object> msg = mapper.readValue(raw, Map.class);

      byte[] rowId = createRowId(msg);
      if (rowId == null) {
        // TODO ... Error Handler
        return;
      }

      Put put = new Put(rowId);
      put.setWriteToWAL(false);

      put.add("raw".getBytes(), new byte[0], raw);

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
    closeConnection();
  }
}
