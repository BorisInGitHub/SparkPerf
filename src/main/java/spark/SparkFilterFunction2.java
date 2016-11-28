package spark;

import org.apache.spark.api.java.function.FilterFunction;

/**
 * Created by breynard on 26/10/16.
 */
public class SparkFilterFunction2 implements FilterFunction<SparkData> {
    private final String filterValue;

    public SparkFilterFunction2(String filterValue) {
        super();
        this.filterValue = filterValue;
    }

    @Override
    public boolean call(SparkData sparkData) throws Exception {
        return !filterValue.equals(sparkData.getId());
    }
}
