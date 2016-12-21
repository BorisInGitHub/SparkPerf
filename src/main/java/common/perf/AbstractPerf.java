package common.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * AbstractPerf
 * Created by breynard on 26/10/16.
 */
public abstract class AbstractPerf {
    private static final String FILE_TEST = "/tmp/dataLong1709858110454081480100.txt";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPerf.class);

    private boolean doCheck = true;

    public abstract String description();

    public abstract void connect() throws Exception;

    public abstract void prepareData(String fileName) throws Exception;

    public abstract List<Pair<String, String>> searchId(String id) throws Exception;

    public abstract int countAll() throws Exception;

    public abstract void close() throws Exception;

    public PerfResult doTestPerf() throws Exception {
        long preparationDuration = -1;
        long searchIdDuration = -1;
        long countDuration = -1;
        LOGGER.warn("-------------------------------");
        LOGGER.warn("Test : {}.", description());
        LOGGER.warn("-------------------------------");

        try {
            LOGGER.info("Connection ...");
            connect();
            LOGGER.info("Préparation des données ...");
            long start = System.currentTimeMillis();
            prepareData(FILE_TEST);
            preparationDuration = System.currentTimeMillis() - start;
            LOGGER.info("Préparation des données en {} ms.", preparationDuration);


            LOGGER.info("Recherche des données ...");
            List<String> ids = Arrays.asList("999888", "98", "999887");
            start = System.currentTimeMillis();
            for (String id : ids) {
                List<Pair<String, String>> pairs = searchId(id);
                if (doCheck) {
                    if (!(pairs != null && pairs.size() == 1 && id.equals(pairs.get(0).fst()) && ("Person_" + id).equals(pairs.get(0).snd()))) {
                        // Error !!!
                        LOGGER.warn("Result for id {} :  {}", id, pairs);
                        throw new RuntimeException("Recherche incorrecte");
                    }
                }
            }
            long duration = System.currentTimeMillis() - start;
            searchIdDuration = duration / ids.size();
            LOGGER.info("Recherche des données en {} ms.", searchIdDuration);


            LOGGER.info("Comptage des données ...");
            start = System.currentTimeMillis();
            int count = countAll();
            countDuration = System.currentTimeMillis() - start;
            if (count == -1) {
                LOGGER.info("Comptage des données : Not Applicable.");
                countDuration = -1;
            } else {
                if (doCheck) {
                    if (count != 10000000) {
                        LOGGER.warn("Error for count {}", count);
                        throw new RuntimeException("Comptage incorrect");
                    }
                }
                LOGGER.info("Comptage des données en {} ms.", countDuration);
            }
        } finally {
            close();
        }
        return new PerfResult(preparationDuration, searchIdDuration, countDuration);
    }
}
