package postgresql;

import common.perf.AbstractPerf;
import common.perf.Pair;
import org.postgresql.ds.PGPoolingDataSource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Docker
 * docker run --name postgres -h postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres:9.6.1
 *
 * puis lancé un client postgres (exemple via docker) et lancer la commande de création de database
 * docker run -it --rm --link postgres:postgres postgres psql -h postgres -U postgres
 *
 * create database test;
 * <p>
 * Created by breynard on 20/12/16.
 */
public class PostgresqlPerf extends AbstractPerf {
    public static final String TABLE_NAME = "personne3";

    private Connection connection;

    @Override
    public String description() {
        return "Postgresql";
    }

    @Override
    public void connect() throws Exception {
        PGPoolingDataSource datasource;

        datasource = new PGPoolingDataSource();
        datasource.setDatabaseName("test");
        datasource.setServerName("localhost");
        datasource.setPortNumber(5432);
        datasource.setUser("postgres");
        datasource.setPassword("mysecretpassword");
        datasource.setMaxConnections(100);

        connection = datasource.getConnection();
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        try (Statement createStatement = connection.createStatement()) {
            createStatement.execute("CREATE TABLE " + TABLE_NAME + " (id varchar(30), name varchar(30))");
        }

        // Lecture des données
        connection.setAutoCommit(false);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            try (Statement statement = connection.createStatement()) {
                for (String line; (line = br.readLine()) != null; ) {
                    int indexChar = line.indexOf(',');
                    String id = line.substring(0, indexChar);
                    String name = line.substring(indexChar + 1);

                    statement.addBatch("INSERT INTO " + TABLE_NAME + " VALUES('" + id + "','" + name + "')");
                }
                statement.executeBatch();
            }
        }

        try (Statement createStatement = connection.createStatement()) {
            createStatement.execute("CREATE INDEX indexId ON " + TABLE_NAME + " (id)");
        }
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        List<Pair<String, String>> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id='" + id + "'")) {
                while (resultSet.next()) {
                    result.add(new Pair<>(resultSet.getString("id"), resultSet.getString("name")));
                }
            }
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("SELECT count(*) FROM " + TABLE_NAME)) {
                if (resultSet.next()) {
                    return resultSet.getInt(1);
                }
            }
        }
        return -1;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
