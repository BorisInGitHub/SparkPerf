package spark_sql;

import java.io.Serializable;

/**
 * Created by breynard on 28/11/16.
 */
public class SparkSQLData implements Serializable {
    private String id;
    private String name;

    public SparkSQLData() {
        super();
    }

    public SparkSQLData(String id, String name) {
        super();
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
