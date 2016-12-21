package common;

import common.perf.AbstractPerf;
import common.perf.PerfResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.SparkLocalPerf;
import spark_sql.SparkSQLPerf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main
 * Created by breynard on 26/10/16.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        doTests(Arrays.<AbstractPerf>asList(
//                new HivePerf(),
                //new HivePerf2(),
                //new HivePerf3(),
                //new HivePerf4()
                //              new HiveWithIndexPerf()
                new SparkLocalPerf()
//                new HBasePerf()
//                new SparkSQLPerf()
//        new SparkSQLCsvLibPerf()
//                new MongoPerf()
//                new PostgresqlPerf()
        ));
    }

    public static void doTests(List<AbstractPerf> performers) throws Exception {
        Map<AbstractPerf, PerfResult> performersResults = new HashMap<>();
        for (AbstractPerf performer : performers) {
            LOGGER.warn("");
            performersResults.put(performer, performer.doTestPerf());
        }

        LOGGER.warn("");
        LOGGER.warn("------------------------------------------------------------------------------");
        LOGGER.warn("");
        for (Map.Entry<AbstractPerf, PerfResult> entry : performersResults.entrySet()) {
            LOGGER.warn("Performer : {}\n \t{}.", entry.getKey().description(), entry.getValue());
        }
    }
}
