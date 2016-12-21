package spark_sql;

import common.Utils;
import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.Metadata;
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
 *
 * Created by breynard on 28/11/16.
 */
public class SparkSQLLocalPerf extends AbstractPerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSQLLocalPerf.class);
    private static final String SPARK_OBJECT_FILE_NAME = "/tmp/spark.pq";

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

//        Configuration result = new Configuration();
//        Class<?> writeSupportClass = ParquetOutputFormat.getWriteSupportClass(result);
//        LOGGER.info("writeSupportClass {}", writeSupportClass);

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

    public void prepareDataOLD(String fileName) throws Exception {
        StructType customSchema = new StructType();
        customSchema = customSchema.add("id", StringType, false, Metadata.empty());
        customSchema = customSchema.add("name", StringType, false, Metadata.empty());

        DataFrame dataFrame = null;
        int bufferSize = 1024;
        List<Row> rows = new ArrayList<>(bufferSize);
        // Lecture des données
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for (String line; (line = br.readLine()) != null; ) {
                int indexChar = line.indexOf(',');
                String id = line.substring(0, indexChar);
                String name = line.substring(indexChar + 1);

                rows.add(RowFactory.create(id, name));
                if (rows.size() >= bufferSize) {
                    DataFrame dataFrame2 = sqlContext.createDataFrame(rows, customSchema);
                    if (dataFrame == null) {
                        dataFrame = dataFrame2;
                    } else {
                        dataFrame = dataFrame.unionAll(dataFrame2);
                    }
                    rows.clear();
                }
            }
        }
        if (!rows.isEmpty()) {
            DataFrame dataFrame2 = sqlContext.createDataFrame(rows, customSchema);
            if (dataFrame == null) {
                dataFrame = dataFrame2;
            } else {
                dataFrame = dataFrame.unionAll(dataFrame2);
            }
            rows.clear();
        }
        if (dataFrame != null) {
            dataFrame.write().parquet(SPARK_OBJECT_FILE_NAME);
        }
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
