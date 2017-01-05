package spark_sql;

import common.Utils;
import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
public class SparkSQLPerf extends AbstractPerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSQLPerf.class);
    private static final String SPARK_OBJECT_FILE_NAME = "/tmp/spark.pq";

    public String PARQUET_EXTENSION = "";

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;

    private final String master;

    /**
     *
     * @param master local[*] ou yarn-client
     */
    public SparkSQLPerf(String master) {
        super();
        this.master = master;
    }

    @Override
    public String description() {
        return "Mode Spark SQL local";
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
        prepareDataSplit(fileName);
//        prepareDataMap(fileName);
    }

    public void prepareDataMap(String fileName) throws Exception {
        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> data = javaSparkContext.textFile(fileName);

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = data.map(new CSVFunction());

        // Apply the schema to the RDD.
        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);
        dataFrame.write().parquet(SPARK_OBJECT_FILE_NAME);
    }

    public void prepareDataSplit(String fileName) throws Exception {
        // On splitte les données pour mieux les répartir sur HDFS (faire avec des partitions désormais ...)
        PARQUET_EXTENSION = File.separator + "*";
        int count = 0;
        StructType customSchema = new StructType();
        customSchema = customSchema.add("id", StringType, false, Metadata.empty());
        customSchema = customSchema.add("name", StringType, false, Metadata.empty());

        int bufferSize = 1024000;
        List<Row> rows = new ArrayList<>(bufferSize);
        DataFrame dataFrame;
        // Lecture des données
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for (String line; (line = br.readLine()) != null; ) {
                int indexChar = line.indexOf(',');
                String id = line.substring(0, indexChar);
                String name = line.substring(indexChar + 1);

                rows.add(RowFactory.create(id, name));
                if (rows.size() >= bufferSize) {
                    dataFrame = sqlContext.createDataFrame(rows, customSchema).unpersist();
                    dataFrame.write().parquet(SPARK_OBJECT_FILE_NAME + File.separator + count++);
                    rows.clear();
                }
            }
        }
        if (!rows.isEmpty()) {
            dataFrame = sqlContext.createDataFrame(rows, customSchema).unpersist();
            dataFrame.write().parquet(SPARK_OBJECT_FILE_NAME + File.separator + count++);
            rows.clear();
        }
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME + PARQUET_EXTENSION);
        List<Row> collect = dataFrame.filter(new Column("id").equalTo(id)).collectAsList();
        List<Pair<String, String>> result = new ArrayList<>(collect.size());
        for (Row row : collect) {
            result.add(new Pair<>(row.getAs("id").toString(), row.getAs("name").toString()));

        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME + PARQUET_EXTENSION);
        return (int) dataFrame.count();
    }

    @Override
    public void close() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.close();
        }
    }
}
