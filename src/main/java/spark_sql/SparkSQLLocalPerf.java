package spark_sql;

import common.Utils;
import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilisation de Spark SQL
 * Created by breynard on 28/11/16.
 */
public class SparkSQLLocalPerf extends AbstractPerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSQLLocalPerf.class);
    private static final String SPARK_OBJECT_FILE_NAME = "/tmp/spark.pq";
    private static final int PAQUET_SIZE = 100000;

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;


    @Override
    public String description() {
        return "Mode Spark SQL local";
    }

    @Override
    public void connect() throws Exception {
        LOGGER.info("Connection à Spark ...");
        SparkConf conf = new SparkConf()
                .setAppName("SparkPerf")
                .setMaster("local[*]")
                // Pas d'UI
                .set("spark.ui.enabled", "false")
                .set("spark.ui.showConsoleProgress", "false")
                // Mémoire
                .setExecutorEnv("SPARK_DRIVER_MEMORY", "4G")
                .setExecutorEnv("SPARK_JAVA_OPTS", "-Xms4g -Xmx4g -XX:MaxPermSize=2g -XX:+UseG1GC");

        SparkContext spark = new SparkContext(conf);
        javaSparkContext = new JavaSparkContext(spark);
        sqlContext = new SQLContext(javaSparkContext);
        LOGGER.info("Connection à Spark OK");

        Configuration result = new Configuration();
        Class<?> writeSupportClass = ParquetOutputFormat.getWriteSupportClass(result);
        LOGGER.info("writeSupportClass {}", writeSupportClass);

        File file = new File(SPARK_OBJECT_FILE_NAME);
        if (file.exists()) {
            Utils.removeDirectory(file);
        }
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        JavaRDD<SparkSQLData> javaRDD = javaSparkContext.textFile(fileName).map(new Function<String, SparkSQLData>() {
            @Override
            public SparkSQLData call(String line) throws Exception {
                int indexChar = line.indexOf(',');
                return new SparkSQLData(line.substring(0, indexChar), line.substring(indexChar + 1));
            }
        });
        DataFrame dataFrame = sqlContext.createDataFrame(javaRDD, SparkSQLData.class);
        dataFrame.write().save(SPARK_OBJECT_FILE_NAME + File.separator);
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        //JavaRDD<SparkData> parallelize = javaSparkContext.objectFile(SPARK_OBJECT_FILE_NAME);
        //DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME + File.separator + '*');
        DataFrame dataFrame = sqlContext.read().load(SPARK_OBJECT_FILE_NAME + File.separator + '*');
        sqlContext.registerDataFrameAsTable(dataFrame, "SparkDATA");
        DataFrame sql = sqlContext.sql("SELECT id, name FROM SparkDATA WHERE id ='" + id + "'");
        List<Row> collect = sql.collectAsList();
        List<Pair<String, String>> result = new ArrayList<>(collect.size());
        for (Row row : collect) {
            result.add(new Pair<>(row.getAs("id").toString(), row.getAs("name").toString()));

        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        //JavaRDD<SparkData> parallelize = javaSparkContext.objectFile(SPARK_OBJECT_FILE_NAME);
        DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME + File.separator + '*');
        sqlContext.registerDataFrameAsTable(dataFrame, "SparkDATA");
        DataFrame sql = sqlContext.sql("SELECT count(*) FROM SparkDATA");
        return sql.first().getInt(0);
    }

    @Override
    public void close() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.close();
        }
    }
}
