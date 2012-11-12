tn = "sa"

fields = IO.readlines('sa-fields')
fields.map! {|x|
  x.strip.downcase
}
fields.sort!


puts "CREATE TABLE #{tn} ("
for c in fields
  puts "#{c} varchar(255) DEFAULT NULL,"
end
puts "id varchar(255) DEFAULT NULL);";

# vim:ts=2 sts=2 ai expandtab
