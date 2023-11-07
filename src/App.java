import static com.mongodb.client.model.Filters.eq;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;

import org.bson.conversions.Bson;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static com.mongodb.client.model.Filters.eq;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;

public class App {

    private static void menu() {
        System.out.println("1. Search 3 closest businesses");
        System.out.println("2. Review a business");
        System.out.println("3. Exit");
    }

    private static void choice1(MongoCollection<Document> collection, Scanner scanner) {
        //lat/long, but database takes long/lat

        System.out.println("Enter your latitude: ");
        double lat = scanner.nextDouble();
        System.out.println("Enter your longitude: ");
        double lon = scanner.nextDouble();
        // find 3 closest businesses
        // display name, address, city, state, categories, stars, and number of reviews(reviews_count)
        // sort by distance
        // using the following command in mongo shell:
        /*
        db.businesses.find({"location.coordinates": {$near: {$geometry: {type: "Point",coordinates: [-81.4780020341, 28.4519912466]}}}},{name: 1,address: 1,city: 1,state: 1,categories: 1,stars: 1,review_count: 1,_id: 0 }).limit(3);
         */


        List<Bson> pipeline = Arrays.asList(
                Aggregates.geoNear(
                        new Point(new Position(lon, lat)),
                        "location.coordinates"
                ),
                Aggregates.project(
                        Projections.fields(
                                Projections.excludeId(),
                                Projections.include("name"),
                                Projections.include("address"),
                                Projections.include("city"),
                                Projections.include("state"),
                                Projections.include("categories"),
                                Projections.include("stars"),
                                Projections.include("review_count")
                        )
                ),
                Aggregates.limit(3)
        );

        collection.aggregate(pipeline).forEach(document -> System.out.println(document.toJson()));


    }

    private static void choice2(MongoCollection<Document> collection, Scanner scanner) {
        // review a business
        // ask for business name, user name, and review text
        // insert a new review into the reviews array
        // using the following command in mongo shell:
        /*
        db.businesses.updateOne({name: "Pita Pit"}, {$push: {reviews: {user_id: "123", user_name: "John", text: "This is a test review"}}});
         */

        // TODO: check this code
        System.out.println("Enter a business name: ");
        String name = scanner.nextLine();
        System.out.println("Enter your user id: ");
        String user_id = scanner.nextLine();
        System.out.println("Enter your user name: ");
        String user_name = scanner.nextLine();
        System.out.println("Enter your review text: ");
        String text = scanner.nextLine();

        Document review = new Document("user_id", user_id)
                .append("user_name", user_name)
                .append("text", text);

        collection.updateOne(eq("name", name), Updates.push("reviews", review));

        System.out.println("Review added successfully");

        //todo: check if review added successfully
    }

    public static void main( String[] args ) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory ();
        Logger rootLogger = loggerContext.getLogger ("org.mongodb.driver");
        rootLogger.setLevel (Level.OFF);

        // Replace the placeholder with your MongoDB deployment's connection string
        String uri = "mongodb+srv://ruoqiuhuang:Hrq%402358558172@cluster0.5e7c2ll.mongodb.net/?retryWrites=true&w=majority";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("hw6");
            MongoCollection<Document> collection = database.getCollection("businesses");

            Scanner scanner = new Scanner(System.in);

            boolean loop = true;
            while (loop) {
                menu();

                System.out.println("Enter a number: ");
                int choice = scanner.nextInt();
                scanner.nextLine();

                switch (choice){
                    case 1:
                        choice1(collection, scanner);
                        break;
                    case 2:
                        choice2(collection, scanner);
                        break;
                    case 3:
                        loop = false;
                        break;
                    default:
                        System.out.println("Invalid input");
                        break;
                }

                /*
                // test code below
                System.out.println("Enter a business name: ");
                String name = scanner.nextLine();
                if (name.isEmpty()) {
                    loop = false;
                }



                Document doc = collection.find(eq("name", name)).first();
                if (doc != null) {
                    System.out.println(doc.toJson());
                } else {
                    System.out.println("No matching documents found.");
                }
                */
            }
        }
    }
}