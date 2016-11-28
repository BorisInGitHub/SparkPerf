package hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * HivePerf
 * Created by breynard on 24/10/16.
 */
public class HivePerf4 extends HivePerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(HivePerf2.class);

    @Override
    public String description() {
        return "Hive brut (sans index ou autre) - Primary Key and Index";
    }

    @Override
    public void prepareData(String fileName) throws SQLException, ClassNotFoundException {
        openConnection();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            //stmt.execute("CREATE TABLE " + TABLE_NAME + " (" + COL_KEY_NAME + " int NOT NULL, " + COL_VALUE_NAME + " string) PRIMARY KEY (" + COL_KEY_NAME + ") UNIQUE KEY MON_INDEX (" + COL_KEY_NAME + "), ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (" + COL_KEY_NAME + " int, " + COL_VALUE_NAME + " string, UNIQUE KEY MON_INDEX (" + COL_KEY_NAME + ")), ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

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
}
