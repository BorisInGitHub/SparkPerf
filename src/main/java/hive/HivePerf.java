package hive;

import common.perf.AbstractPerf;
import common.perf.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * HivePerf
 * Created by breynard on 24/10/16.
 */
public class HivePerf extends AbstractPerf {
    protected static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    protected static final String TABLE_NAME = "tableperfhive";
    protected static final String COL_KEY_NAME = "colkey";
    protected static final String COL_VALUE_NAME = "colvalue";

    private static final Logger LOGGER = LoggerFactory.getLogger(HivePerf.class);

    protected Connection connection;

    @Override
    public String description() {
        return "Hive brut (sans index ou autre)";
    }

    protected void openConnection() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_NAME);

        //replace "hive" here with the name of the user the queries should run as
        connection = DriverManager.getConnection("jdbc:hive2://192.168.1.246:10000/default", "mapr", "mapr");
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            }
            connection.close();
        }
    }

    @Override
    public void connect() throws Exception {
        openConnection();
    }

    @Override
    public void prepareData(String fileName) throws SQLException, ClassNotFoundException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (" + COL_KEY_NAME + " int, " + COL_VALUE_NAME + " string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

            // show tables
            String sql = "SHOW TABLES '" + TABLE_NAME + "'";
            try (ResultSet res = stmt.executeQuery(sql)) {
                if (res.next()) {
                    LOGGER.debug("Tables in Hive {}", res.getString(1));
                }
            }

            // describe table
            sql = "DESCRIBE " + TABLE_NAME;
            try (ResultSet res = stmt.executeQuery(sql)) {
                while (res.next()) {
                    LOGGER.debug("Table description: {}\t{}", res.getString(1), res.getString(2));
                }
            }

            // load data into table
            // NOTE: filepath has to be local to the hive server
            // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
            sql = "LOAD DATA LOCAL INPATH '" + fileName + "' INTO TABLE " + TABLE_NAME;
            stmt.execute(sql);
        }
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws SQLException {
        List<Pair<String, String>> result = new ArrayList<>();
        String sql = "SELECT " + COL_KEY_NAME + ", " + COL_VALUE_NAME + " FROM " + TABLE_NAME + " WHERE " + COL_KEY_NAME + "=" + id;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet res = stmt.executeQuery(sql)) {
                while (res.next()) {
                    result.add(new Pair<>(res.getString(1), res.getString(2)));
                }
            }
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        String sql = "SELECT count(1) FROM " + TABLE_NAME;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet res = stmt.executeQuery(sql)) {
                if (res.next()) {
                    return res.getInt(1);
                }
            }
        }
        return -1;
    }
}
