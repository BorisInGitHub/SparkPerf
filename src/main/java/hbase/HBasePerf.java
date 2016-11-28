package hbase;

import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by breynard on 27/10/16.
 */
public class HBasePerf extends AbstractPerf {
    protected static final String TABLE_NAME = "tablehbase";
    protected static final String COL_KEY_NAME = "colkey";
    protected static final String COL_VALUE_NAME = "colvalue";
    private static final Logger LOGGER = LoggerFactory.getLogger(HBasePerf.class);
    private Configuration configuration;


    @Override
    public String description() {
        return "HBase";
    }

    @Override
    public void connect() throws Exception {
        configuration = HBaseConfiguration.create();
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        createTable(TABLE_NAME, Arrays.asList(COL_VALUE_NAME));

        // Lecture des données
        HTable table = new HTable(configuration, TABLE_NAME);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for (String line; (line = br.readLine()) != null; ) {
                int indexChar = line.indexOf(',');
                String id = line.substring(0, indexChar);
                String name = line.substring(indexChar + 1);

                Put put = new Put(Bytes.toBytes(id));
                put.add(Bytes.toBytes(COL_VALUE_NAME), Bytes.toBytes(""), Bytes.toBytes(name));
                table.put(put);
            }
        }
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        HTable table = new HTable(configuration, TABLE_NAME);
        Get get = new Get(id.getBytes());
        Result rs = table.get(get);
        List<Pair<String, String>> result = new ArrayList<>();
        for (KeyValue kv : rs.raw()) {
            result.add(new Pair<>(new String(kv.getRow()), new String(kv.getValue())));
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        return -1;
    }

    @Override
    public void close() throws Exception {

    }

    /**
     * Create a table
     */
    public void createTable(String tableName, List<String> familys) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists(tableName)) {
            LOGGER.warn("Table already exists");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (String family : familys) {
                tableDesc.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(tableDesc);
        }
    }

    /**
     * Delete a table
     */
    public void deleteTable(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * Put (or insert) a row
     */
    public void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception {
        HTable table = new HTable(configuration, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                .toBytes(value));
        table.put(put);
    }

    /**
     * Delete a row
     */
    public void delRecord(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(configuration, tableName);
        List<Delete> list = new ArrayList<>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
    }

    /**
     * Get a row
     */
    public void getOneRecord(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(configuration, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * Scan (or list) a table
     */
    public void getAllRecord(String tableName) throws IOException {
        HTable table = new HTable(configuration, tableName);
        Scan s = new Scan();
        ResultScanner ss = table.getScanner(s);
        for (Result r : ss) {
            for (KeyValue kv : r.raw()) {
                System.out.print(new String(kv.getRow()) + " ");
                System.out.print(new String(kv.getFamily()) + ":");
                System.out.print(new String(kv.getQualifier()) + " ");
                System.out.print(kv.getTimestamp() + " ");
                System.out.println(new String(kv.getValue()));
            }
        }
    }
}
