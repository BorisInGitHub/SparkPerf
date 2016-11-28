package spark;

import org.apache.spark.api.java.function.Function;

/**
 * SparkFilterFunction
 * Created by breynard on 26/10/16.
 */
public class SparkFilterFunction implements Function<SparkData, Boolean> {
    private final String filterValue;

    public SparkFilterFunction(String filterValue) {
        super();
        this.filterValue = filterValue;
    }

    @Override
    public Boolean call(SparkData v1) throws Exception {
        return v1 != null && filterValue.equals(v1.getId());
    }
}
