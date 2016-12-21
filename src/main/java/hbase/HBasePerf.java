package hbase;

import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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
 * https://hub.docker.com/r/dajobe/hbase/
 * <p>
 * Docker
 * docker run --name=hbase-docker -h hbase-docker  -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 2181:2181 -p 16010:16010 -d -v $PWD/data:/data dajobe/hbase
 * <p>
 * # REST API
 * EXPOSE 8080
 * # REST Web UI at :8085/rest.jsp
 * EXPOSE 8085
 * # Thrift API
 * EXPOSE 9090
 * # Thrift Web UI at :9095/thrift.jsp
 * EXPOSE 9095
 * # HBase's Embedded zookeeper cluster
 * EXPOSE 2181
 * # HBase Master web UI at :16010/master-status;  ZK at :16010/zk.jsp
 * EXPOSE 16010
 * <p>
 * <p>
 * git clone https://github.com/dajobe/hbase-docker.git
 * cd hbase-docker
 * ./start-hbase.sh
 * <p>
 * docker stop hbase-docker
 * docker rm hbase-docker
 * sudo rm -rf data
 * <p>
 * Created by breynard on 27/10/16.
 */
public class HBasePerf extends AbstractPerf {
    protected static final String TABLE_NAME = "tablehbase";
    protected static final String COL_KEY_NAME = "colkey";
    protected static final String COL_VALUE_NAME = "colvalue";
    private static final Logger LOGGER = LoggerFactory.getLogger(HBasePerf.class);

    private Connection connection;


    @Override
    public String description() {
        return "HBase";
    }

    @Override
    public void connect() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.master", "*" + "hbase-docker" + ":16010*");
        configuration.set("hbase.zookeeper.quorum", "hbase-docker");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(configuration);
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        createTable(TABLE_NAME, Arrays.asList(COL_VALUE_NAME));

        // Lecture des données
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        int bufferSize = 1024;
        List<Put> puts = new ArrayList<>(bufferSize);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for (String line; (line = br.readLine()) != null; ) {
                int indexChar = line.indexOf(',');
                String id = line.substring(0, indexChar);
                String name = line.substring(indexChar + 1);

                Put put = new Put(Bytes.toBytes(id));
                put.addColumn(Bytes.toBytes(COL_VALUE_NAME), Bytes.toBytes(COL_VALUE_NAME), Bytes.toBytes(name));
                puts.add(put);

                if (puts.size() >= bufferSize) {
                    table.put(puts);
                    puts.clear();
                }

            }
            if (!puts.isEmpty()) {
                table.put(puts);
                puts.clear();
            }
        }
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        List<Pair<String, String>> result = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        // Create a new Get request and specify the rowId passed by the user.
        Result resultTable = table.get(new Get(id.getBytes()));

        // Iterate of the results. Each Cell is a value for column
        // so multiple Cells will be processed for each row.
        for (Cell cell : resultTable.listCells()) {
            // We use the CellUtil class to clone values
            // from the returned cells.
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));

            if (column.equals(COL_VALUE_NAME) || family.equals(COL_VALUE_NAME))
                result.add(new Pair<String, String>(row, value));
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        return -1;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Create a table
     */
    public void createTable(String tableName, List<String> familys) throws Exception {
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (String colFamily : familys) {
            tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
        }
        admin.createTable(tableDescriptor);
    }

    /**
     * Delete a table
     */
    public void deleteTable(String tableName) throws Exception {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * Put (or insert) a row
     */
    public void addRecord(String tableName, String rowKey, String colName, String value) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        addRecord(table, rowKey, colName, value);
    }

    private void addRecord(Table table, String rowKey, String colName, String value) throws IOException {
        // Create a new Put request.
        Put put = new Put(Bytes.toBytes(rowKey));

        // Here we add only one column value to the row but
        // multiple column values can be added to the row at
        // once by calling this method multiple times.
        put.addColumn(Bytes.toBytes(colName), Bytes.toBytes(colName), Bytes.toBytes(value));

        // Execute the put on the table.
        table.put(put);
    }

    /**
     * Delete a row
     */
    public void delRecord(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
    }

    /**
     * Get a row
     */
    public void getOneRecord(String tableName, String rowKey) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        // Create a new Get request and specify the rowId passed by the user.
        Result result = table.get(new Get(rowKey.getBytes()));

        // Iterate of the results. Each Cell is a value for column
        // so multiple Cells will be processed for each row.
        for (Cell cell : result.listCells()) {
            // We use the CellUtil class to clone values
            // from the returned cells.
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            System.out.printf("%-20s column=%s:%s, timestamp=%s, value=%s\n", row, family, column, timestamp, value);
        }
    }

    /**
     * Scan (or list) a table
     */
    public void getAllRecord(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
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
