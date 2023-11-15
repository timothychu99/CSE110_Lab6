package cse110.lab;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;


import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;


public class Delete {


    public static void main(String[] args) {
        String uri = "mongodb+srv://TimChu:7Ai3WLXEeBnzZ%3Ap@cluster0.xoawptd.mongodb.net/?retryWrites=true&w=majority";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades");


            // delete one document
            Bson filter = eq("Recipe", 10000);
            DeleteResult result = gradesCollection.deleteOne(filter);
            System.out.println(result);


            // findOneAndDelete operation
            filter = eq("Recipe", 10002);
            Document doc = gradesCollection.findOneAndDelete(filter);
            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));


            // delete many documents
            filter = gte("Recipe", 10000);
            result = gradesCollection.deleteMany(filter);
            System.out.println(result);


            // delete the entire collection and its metadata (indexes, chunk metadata, etc).
            //gradesCollection.drop();
        }
    }
}

