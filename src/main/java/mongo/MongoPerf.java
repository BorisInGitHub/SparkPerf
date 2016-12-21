package mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import common.perf.AbstractPerf;
import common.perf.Pair;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mongo Perf
 * <p>
 * Docker Mongo
 * docker run -i -t --name mongoDocker -h mongoDocker -p 27017:27017 --net="bridge" mongo:3.4 --quiet --nojournal
 * <p>
 * Created by breynard on 20/12/16.
 */
public class MongoPerf extends AbstractPerf {
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "TEST2";
    private static final int ASCENDING = 1;
    private static final String MONGO_ROWID_COLUMN_NAME = "ID";
    private static final String MONGO_ROWNAME_COLUMN_NAME = "NAME";


    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;
    private String collectionName;


    @Override
    public String description() {
        return "Mongo";
    }

    @Override
    public void connect() throws Exception {
        // https://docs.mongodb.com/manual/reference/connection-string/
        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
        mongoDatabase = mongoClient.getDatabase(DATABASE_NAME);
        collectionName = "maCollection";
    }

    @Override
    public void prepareData(String fileName) throws Exception {
        mongoDatabase.createCollection(collectionName,
                new CreateCollectionOptions()
                        .autoIndex(false)
                        .validationOptions(new ValidationOptions().validationLevel(ValidationLevel.OFF)));
        mongoCollection = mongoDatabase.getCollection(collectionName);

        mongoCollection.createIndex(
                new BasicDBObject(MONGO_ROWID_COLUMN_NAME, ASCENDING),
                new IndexOptions().unique(true));
//        mongoCollection.createIndex(
//                new BasicDBObject(MONGO_ROWNAME_COLUMN_NAME, ASCENDING),
//                new IndexOptions().unique(false));


        int bufferSize = 1024;
        List<Document> documents = new ArrayList<>(bufferSize);
        // Lecture des données
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for (String line; (line = br.readLine()) != null; ) {
                int indexChar = line.indexOf(',');
                String id = line.substring(0, indexChar);
                String name = line.substring(indexChar + 1);

                Map<String, Object> data = new HashMap<>(2);
                data.put(MONGO_ROWID_COLUMN_NAME, id);
                data.put(MONGO_ROWNAME_COLUMN_NAME, name);

                documents.add(new Document(data));
                if (documents.size() >= bufferSize) {
                    mongoCollection.insertMany(documents);
                    documents.clear();
                }
            }

            if (!documents.isEmpty()) {
                mongoCollection.insertMany(documents);
                documents.clear();
            }
        }
    }

    @Override
    public List<Pair<String, String>> searchId(String id) throws Exception {
        List<Pair<String, String>> result = new ArrayList<>();
        FindIterable<Document> filter = mongoCollection.find().filter(Filters.eq(MONGO_ROWID_COLUMN_NAME, id));
        for (Document document : filter) {
            result.add(new Pair<>(document.getString(MONGO_ROWID_COLUMN_NAME), document.getString(MONGO_ROWNAME_COLUMN_NAME)));
        }
        return result;
    }

    @Override
    public int countAll() throws Exception {
        return (int) mongoCollection.count();
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
