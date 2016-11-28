package spark;

import common.Utils;
import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * SparkTest
 * Created by breynard on 26/10/16.
 */
public class SparkLocalPerf extends AbstractPerf {
    public static final Encoder<SparkData> SPARK_DATA_ENCODER = Encoders.bean(SparkData.class);
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkLocalPerf.class);
    private static final String SPARK_OBJECT_FILE_NAME = "/tmp/spark.pq";
    private static final int PAQUET_SIZE = 100000;

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;


    @Override
    public String description() {
        return "Mode Spark local";
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
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            int paquetIndex = 0;
            List<SparkData> datas = new ArrayList<>(PAQUET_SIZE);
            int datasIndex = 0;
            for (String line; (line = br.readLine()) != null; ) {
                int indexChar = line.indexOf(',');
                datas.add(new SparkData(line.substring(0, indexChar), line.substring(indexChar + 1)));

                datasIndex++;
                if (datasIndex >= PAQUET_SIZE) {
                    saveData(datas, paquetIndex++);
                    datas.clear();
                    datasIndex = 0;
                }
            }
            if (!datas.isEmpty()) {
                saveData(datas, paquetIndex++);
            }
        }
    }

    public void saveData(List<SparkData> datas, int paquetIndex) {
        // Persistance des données dans spark
        //JavaRDD<SparkData> parallelize = javaSparkContext.parallelize(datas);
        //parallelize.saveAsObjectFile(SPARK_OBJECT_FILE_NAME);
        DataFrame dataFrame = sqlContext.createDataFrame(datas, SparkData.class);
        //dataFrame.write().parquet(SPARK_OBJECT_FILE_NAME + File.separator + paquetIndex);
        dataFrame.write().save(SPARK_OBJECT_FILE_NAME + File.separator + paquetIndex);
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        //JavaRDD<SparkData> parallelize = javaSparkContext.objectFile(SPARK_OBJECT_FILE_NAME);
        //DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME + File.separator + '*');
        DataFrame dataFrame = sqlContext.read().load(SPARK_OBJECT_FILE_NAME + File.separator + '*');
        // Sélection du bon ID
        List<SparkData> collect = dataFrame.as(SPARK_DATA_ENCODER).filter(new SparkFilterFunction2(id)).collectAsList();

        List<Pair<String, String>> result = new ArrayList<>(collect.size());
        for (SparkData sparkData : collect) {
            result.add(new Pair<>(sparkData.getId(), sparkData.getName()));
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        //JavaRDD<SparkData> parallelize = javaSparkContext.objectFile(SPARK_OBJECT_FILE_NAME);
        DataFrame dataFrame = sqlContext.read().parquet(SPARK_OBJECT_FILE_NAME + File.separator + '*');
        return (int) dataFrame.count();
    }

    @Override
    public void close() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.close();
        }
    }
}
