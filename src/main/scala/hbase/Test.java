package hbase;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class Test {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("C:\\Users\\User\\IdeaProjects\\ScalaSpark\\hbase-site.xml"));


        try {
            byte[] key = Bytes.toBytes("row-by-java-client");
            byte[] val = Bytes.toBytes("val");

            HTable table = new HTable(conf, "sample");
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("sample"));
            tableDescriptor.addFamily(new HColumnDescriptor("data"));
            admin.createTable(tableDescriptor);
            boolean tableAvailable = admin.isTableAvailable("sample");
            System.out.println("tableAvailable = " + tableAvailable);
            Put p = new Put(key);
            byte[] family = Bytes.toBytes("data");
            byte[] column = Bytes.toBytes("column");
            p.add(family, column, val);
            table.put(p);

            Get g = new Get(key);
            Result r = table.get(g);
            System.out.println("Get: " + r);

            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            try {
                for (Result sr: scanner)
                    System.out.println("Scan: " + sr);
            } finally {
                scanner.close();
            }

            byte[] start = Bytes.toBytes("row3");
            scan = new Scan(start);
            scanner = table.getScanner(scan);
            try {
                for (Result sr: scanner)
                    System.out.println("Scan: " + sr);
            } finally {
                scanner.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}