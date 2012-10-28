include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.io.Text


conf = HBaseConfiguration.new()
admin = HBaseAdmin.new(conf)

tablename = 'sa' 

desc = HTableDescriptor.new(tablename)
cmp = HColumnDescriptor.new("mp")
craw = HColumnDescriptor.new("raw")
ca = HColumnDescriptor.new("a")

[cmp, craw, ca].each do |c|
  c.setCompressionType(Compression::Algorithm::SNAPPY)
  desc.addFamily(c)
end

#----
if admin.tableExists(tablename)
  admin.disableTable(tablename)
  admin.deleteTable(tablename)
end

admin.createTable(desc)

exit

# vim: ts=2 sts=2 ai
