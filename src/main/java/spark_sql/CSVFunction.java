package spark_sql;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import java.io.Serializable;

/**
 * Created by breynard on 21/12/16.
 */
public class CSVFunction implements Serializable, Function<String, Row> {
    public Row call(String record) throws Exception {
        String[] fields = record.split(",");
        return new GenericRow(fields);
    }
}
