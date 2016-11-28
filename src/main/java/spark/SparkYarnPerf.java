package spark;

import common.perf.AbstractPerf;
import common.perf.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * SparkTest
 * Created by breynard on 26/10/16.
 */
public class SparkYarnPerf extends AbstractPerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkYarnPerf.class);
    private static final String SPARK_OBJECT_FILE_NAME = "/tmp/spark.pq";

    private JavaSparkContext javaSparkContext;


    @Override
    public String description() {
        return "Mode Spark yarn";
    }

    @Override
    public void connect() throws Exception {
        LOGGER.info("Connection à Spark ...");
        SparkConf conf = new SparkConf()
                .setAppName("SparkPerf")
                .setMaster("yarn-client")
                // Pas d'UI
                .set("spark.ui.enabled", "false")
                .set("spark.ui.showConsoleProgress", "false");

        SparkContext spark = new SparkContext(conf);
        javaSparkContext = new JavaSparkContext(spark);
        LOGGER.info("Connection à Spark OK");
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        List<SparkData> datas = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for (String line; (line = br.readLine()) != null; ) {
                int index = line.indexOf(',');
                datas.add(new SparkData(line.substring(0, index), line.substring(index + 1)));
            }
        }

        // Persistance des données dans spark
        JavaRDD<SparkData> parallelize = javaSparkContext.parallelize(datas);
        parallelize.saveAsObjectFile(SPARK_OBJECT_FILE_NAME);
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        JavaRDD<SparkData> parallelize = javaSparkContext.objectFile(SPARK_OBJECT_FILE_NAME);
        // Sélection du bon ID
        List<SparkData> collect = parallelize.filter(new SparkFilterFunction(id)).collect();

        List<Pair<String, String>> result = new ArrayList<>(collect.size());
        for (SparkData sparkData : collect) {
            result.add(new Pair<>(sparkData.getId(), sparkData.getName()));
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        JavaRDD<SparkData> parallelize = javaSparkContext.objectFile(SPARK_OBJECT_FILE_NAME);
        return (int) parallelize.count();
    }

    @Override
    public void close() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.close();
        }
    }
}
