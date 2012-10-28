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
def hex(s)
	return s.bytes.map {|c| '%02x' % c}.join
end

tsbegin = Time.gm(2012, 8, 10)
tsend = Time.gm(2012, 10, 30)
tsadd = 3600 * 6

splits = Array.new

while tsbegin < tsend do
	#splits << hex(tsbegin.strftime("%Y%m%d%H%M")).to_java_bytes
	splits << tsbegin.strftime("%Y%m%d%H%M").to_java_bytes
	tsbegin += tsadd
end


if admin.tableExists(tablename)
  admin.disableTable(tablename)
  admin.deleteTable(tablename)
end

splitkeys = Java::byte[][splits.size].new
for i in 0 .. splits.size - 1 do
	splitkeys[i] = splits[i]
end
admin.createTable(desc, splitkeys)

exit

# vim: ts=2 sts=2 ai
