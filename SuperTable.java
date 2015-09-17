import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SuperTable {

   public static final String POWERS = "powers";
   public static final String PERSONAL = "personal";
   public static final String PROFESSIONAL = "professional";
   public static final String HERO = "hero";
   public static final String POWER = "power";
   public static final String NAME = "name";
   public static final String XP = "xp";

   public static void main(String[] args) throws IOException {
      final Configuration config = HBaseConfiguration.create();
      createTable(config);
      insertData(config);
      printData(config);
   }

   private static void printData(Configuration config) throws IOException {
      final HTable table = new HTable(config, POWERS);
      final Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes(PERSONAL), Bytes.toBytes(HERO));
      final ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
         System.out.println(result);
      }
      scanner.close();
      table.close();
   }

   private static void insertData(Configuration config) throws IOException {
      final HTable hTable = new HTable(config, POWERS);
      hTable.put(createPut("row1", "superman", "strength", "clark", "100"));
      hTable.put(createPut("row2", "batman", "money", "bruce", "50"));
      hTable.put(createPut("row3", "wolverine", "healing", "logan", "75"));
      hTable.close();
   }

   private static Put createPut(String rowName, String hero, String power, String name, String xp) {
      final Put p = new Put(Bytes.toBytes(rowName));
      p.add(Bytes.toBytes(PERSONAL), Bytes.toBytes(HERO), Bytes.toBytes(hero));
      p.add(Bytes.toBytes(PERSONAL), Bytes.toBytes(POWER), Bytes.toBytes(power));
      p.add(Bytes.toBytes(PROFESSIONAL), Bytes.toBytes(NAME), Bytes.toBytes(name));
      p.add(Bytes.toBytes(PROFESSIONAL), Bytes.toBytes(XP), Bytes.toBytes(xp));
      return p;
   }

   private static void createTable(Configuration conig) throws IOException {
      final HBaseAdmin admin = new HBaseAdmin(conig);
      final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(POWERS));
      tableDescriptor.addFamily(new HColumnDescriptor(PERSONAL));
      tableDescriptor.addFamily(new HColumnDescriptor(PROFESSIONAL));
      admin.createTable(tableDescriptor);
   }
}

