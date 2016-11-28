package hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/***
 * HiveWithIndexPerf
 * Created by breynard on 24/10/16.
 */
public class HiveWithIndexPerf extends HivePerf {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveWithIndexPerf.class);

    private static final String INDEX_NAME = "indexcolkey";

    @Override
    public String description() {
        return "Hive avec index";
    }

    @Override
    public void prepareData(String fileName) throws SQLException, ClassNotFoundException {
        openConnection();

        /**
         * Rq : Ajout dans /opt/mapr/hive/hive-1.2/conf/hive-site.xml
<property>
  <name>hive.optimize.autoindex</name>
  <value>true</value>
</property>

       Redémarrage Hive :  maprcli node services -name hs2 -action restart -nodes maprdemo

         */

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP INDEX IF EXISTS " + INDEX_NAME + " ON " + TABLE_NAME);
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);

            // Table External ou pas ???
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (" + COL_KEY_NAME + " int, " + COL_VALUE_NAME + " string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

            // create index
            String sql = "CREATE INDEX " + INDEX_NAME + " ON TABLE " + TABLE_NAME + " (" + COL_KEY_NAME + ") AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD";
            stmt.execute(sql);

//            // alter Table
//            sql = "ALTER INDEX " + INDEX_NAME + " ON " + TABLE_NAME + " REBUILD";
//            stmt.execute(sql);

            // show tables
            sql = "SHOW TABLES '" + TABLE_NAME + "'";
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

            // show index
            sql = "SHOW INDEX ON " + TABLE_NAME;
            try (ResultSet res = stmt.executeQuery(sql)) {
                while (res.next()) {
                    LOGGER.debug("Table index: {}\t{}", res.getString(1), res.getString(2));
                }
            }

            // load data into table
            // NOTE: filepath has to be local to the hive server
            // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
            sql = "LOAD DATA LOCAL INPATH '" + fileName + "' INTO TABLE " + TABLE_NAME;
            stmt.execute(sql);

            // Le rebuild est nécessaire    https://cwiki.apache.org/confluence/display/Hive/IndexDev#IndexDev-CREATEINDEX
            sql = "ALTER INDEX " + INDEX_NAME + " ON " + TABLE_NAME + " REBUILD";
            stmt.execute(sql);
        }
    }
}
