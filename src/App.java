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

    private static void choice2(MongoDatabase database, Scanner scanner) {
        MongoCollection<Document> businesses = database.getCollection("businesses");
        MongoCollection<Document> reviews = database.getCollection("reviews");

        // review a business
        // search the business by business_id
        // if not found, print "No matching documents found."
        // if found, add a review to the business
        // review should be an integer from 1 to 5
        // if not, print "Invalid input"
        // if valid, add the review to the business
        // (stars * review_count + new_review_score) /(review_count + 1)
        // update the review_count
        // Store the information of the reviewed business in a collection called reviews
        // if the business is already in the collection, update the review

        System.out.println("Enter a business id: ");
        String business_id = scanner.nextLine();
        Document doc = businesses.find(eq("business_id", business_id)).first();
        if(doc!= null){
            System.out.println(doc.toJson());
            System.out.println("Enter a review score: ");
            int new_review_score = scanner.nextInt();
            if(new_review_score>=1 && new_review_score<=5){
                // update review count
                int review_count = doc.getInteger("review_count");
                review_count++;
                // update stars
                // try getting a double, if not, get an integer
                // then cast to double
                double stars;
                try {
                    stars = doc.getDouble("stars");
                }catch (Exception e){
                    int stars_int = doc.getInteger("stars");
                    stars = (double) stars_int;
                }
                double new_stars = (stars * (review_count-1) + new_review_score) /(review_count);
                // update the business
                businesses.updateOne(
                        Filters.eq("business_id", business_id),
                        Updates.combine(
                                Updates.set("stars", new_stars),
                                Updates.inc("review_count", 1)
                        )
                );
                // update reviews
                Document reviewed_business = businesses.find(eq("business_id", business_id)).first(); //get the reviewed business
                Document review = reviews.find(eq("business_id", business_id)).first();
                if (review != null){
                    reviews.updateOne(
                            Filters.eq("business_id", business_id),
                            Updates.combine(
                                    Updates.set("stars", new_stars),
                                    Updates.inc("review_count", 1)
                            )
                    );
                }else {
                    assert reviewed_business != null;
                    reviews.insertOne(reviewed_business);
                }
            }else {
                System.out.println("Invalid input");
            }
        }else {
            System.out.println("No matching business found");
        }
    }

    public static void main( String[] args ) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory ();
        Logger rootLogger = loggerContext.getLogger ("org.mongodb.driver");
        rootLogger.setLevel (Level.OFF);

        // Replace the placeholder with your MongoDB deployment's connection string
        String uri = "mongodb+srv://ruoqiuhuang:RoyAtlashw6@cluster0.5e7c2ll.mongodb.net/?retryWrites=true&w=majority";

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
                        choice2(database, scanner);
                        break;
                    case 3:
                        loop = false;
                        break;
                    default:
                        System.out.println("Invalid input");
                        break;
                }
            }
        }
    }
}