package cse110.lab;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;

import java.io.BufferedReader;  
import java.io.FileReader;  
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.bulk.UpdateRequest;  

public class RecipeLab {
    public static void main(String[] args) {  

        // 1. Read Recipe Data
        String line = "";  
        String splitBy = ";";  
        List<String[]> recipeList = new ArrayList<String[]>();
        try {  
            //parsing a CSV file into BufferedReader class constructor  
            BufferedReader br = new BufferedReader(new FileReader("app\\src\\main\\resources\\recipes.csv")); 
            while ((line = br.readLine()) != null) {  
                String[] recipe = line.split(splitBy);    // use comma as separator  
                recipeList.add(recipe);
            }  
        } 
        catch (IOException e) { 
            e.printStackTrace();  
        }    

// -----------------------------------------------------------------------------------        
        //2. Connect to MongoDB Cluster
        String uri = "mongodb+srv://TimChu:7Ai3WLXEeBnzZ%3Ap@cluster0.xoawptd.mongodb.net/?retryWrites=true&w=majority";
        try (MongoClient mongoClient = MongoClients.create(uri)) {

            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("recipe_db");
            MongoCollection<Document> recipeCollection = sampleTrainingDB.getCollection("recipes");

            //3. Insert recipe into the recipeCollection
            List<Document> recipes = new ArrayList<>();
            int countChanges = 0;
            for (int i = 1; i < recipeList.size(); i++) {

                Bson filter = eq("Recipe", recipeList.get(i)[0]);
                
                if (recipeCollection.find(filter).first() == null) {
                    recipes.add(new Document("_id", new ObjectId())
                        .append("Recipe", recipeList.get(i)[0])
                        .append("Description", recipeList.get(i)[1])
                        .append("Hours", Double.parseDouble(recipeList.get(i)[2])));
                    countChanges++;
                }
            }
            if(countChanges > 0){
                recipeCollection.insertMany(recipes, new InsertManyOptions().ordered(true));
            }
            // 4. Print Recipe Count
            System.out.println("Documents: " + recipeCollection.countDocuments());

            // 5. Read Operation
            List<Document> readRecipeList = recipeCollection.find(eq("Recipe", "Savory Spinach Delight")).into(new ArrayList<>());
            for (Document recipe : readRecipeList) {
                System.out.println("Hours: " + recipe.get("Hours") + " for Savory Spinach Delight");
            }

            // 6. Update Operation
//            FindOneAndUpdateOptions optionAfter = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
            Bson filter = eq("Recipe", "Savory Spinach Delight");
            Bson update1 = set("Hours",4.5); //increment by 3
            // returns the old version of the document before the update.
            UpdateOptions options = new UpdateOptions().upsert(true);

            UpdateResult updateResult = recipeCollection.updateOne(filter, update1, options);

            
            JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();

            Document recipe = recipeCollection.find(filter).first();
            
            // 7. Print Updated Hours
            System.out.println("Updated Hours: " + recipe.get("Hours") + " for Savory Spinach Delight");
  
            // 8. Delete operation
            String toDelete = "Spicy Shrimp Tacos";
            Bson deleteFilter = eq("Recipe", toDelete);
            recipeCollection.deleteOne(deleteFilter);
            
            // 9. Print out number of recipes left
             System.out.println("Documents: " + recipeCollection.countDocuments());
        }
    }


    
}
