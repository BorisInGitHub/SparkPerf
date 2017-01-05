package spark_sql;

import common.Utils;
import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Utilisation de Spark SQL
 * Effacer les précédentes données
 * rm -rf /tmp/spark.pq
 * <p>
 * Created by breynard on 28/11/16.
 */
public class SparkSQLCsvLibPerf extends AbstractPerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSQLPerf.class);
    private static final String SPARK_OBJECT_FILE_NAME = "/tmp/spark.pq";
    private final String master;
    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;

    /**
     * @param master local[*] ou yarn-client
     */
    public SparkSQLCsvLibPerf(String master) {
        super();
        this.master = master;
    }

    @Override
    public String description() {
        return "Mode Spark SQL (avec librairie CSV databricks)";
    }

    @Override
    public void connect() throws Exception {
        LOGGER.info("Connection à Spark ...");
        SparkConf conf = new SparkConf()
                .setAppName("SparkPerf")
                .setMaster(master)
                // Pas d'UI
                .set("spark.ui.enabled", "false")
                .set("spark.ui.showConsoleProgress", "false")
                // Mémoire
                .setExecutorEnv("SPARK_DRIVER_MEMORY", "4G")
                .setExecutorEnv("SPARK_JAVA_OPTS", "-Xms4g -Xmx4g -XX:MaxPermSize=2g -XX:+UseG1GC")


                .set("spark.yarn.dist.files", "maprfs://demo.mapr.com/user/spark/hivePerf-1.0-SNAPSHOT-worker.jar")
                .set("spark.yarn.jar", "maprfs://demo.mapr.com/user/spark/spark-assembly.jar")
                .set("spark.yarn.am.extraLibraryPath", "maprfs://demo.mapr.com/user/spark/hivePerf-1.0-SNAPSHOT-worker.jar");

        SparkContext spark = new SparkContext(conf);
        javaSparkContext = new JavaSparkContext(spark);
        sqlContext = new SQLContext(javaSparkContext);
        LOGGER.info("Connection à Spark OK");

        File file = new File(SPARK_OBJECT_FILE_NAME);
        if (file.exists()) {
            Utils.removeDirectory(file);
        }
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        StructType customSchema = new StructType();
        customSchema = customSchema.add("id", StringType, false, Metadata.empty());
        customSchema = customSchema.add("name", StringType, false, Metadata.empty());

        DataFrame dataFrame = sqlContext
                .read()
                .format("com.databricks.spark.csv")
                .option("header", Boolean.toString(false))
                .option("delimiter", ",")
                .schema(customSchema)
                .load(fileName);

        dataFrame.write().parquet(SPARK_OBJECT_FILE_NAME);
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME);
        List<Row> collect = dataFrame.filter(new Column("id").equalTo(id)).collectAsList();
        List<Pair<String, String>> result = new ArrayList<>(collect.size());
        for (Row row : collect) {
            result.add(new Pair<>(row.getAs("id").toString(), row.getAs("name").toString()));

        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME);
        return (int) dataFrame.count();
    }

    @Override
    public void close() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.close();
        }
    }
}
