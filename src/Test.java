import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;


public class Test {
    public static void main(String[] args) {

        String uri = "mongodb+srv://ruoqiuhuang:Hrq%402358558172@cluster0.5e7c2ll.mongodb.net/?retryWrites=true&w=majority";

        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase("hw6");
        MongoCollection<Document> collection = database.getCollection("businesses");
        collection.find().first();


    }
}
