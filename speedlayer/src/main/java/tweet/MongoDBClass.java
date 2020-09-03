package speedlayer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.elasticsearch.client.RestHighLevelClient;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;  

//Connect to Database
public class MongoDBClass  { 
	
	private static MongoClient mongo;
	private static MongoDatabase database;
	private static MongoCollection<Document> collection;
	
	private static synchronized MongoDatabase makeConnection() {
		
		if (database == null) {
			mongo = new MongoClient( "localhost" , 27017 ); 
	      // Creating Credentials 
	      MongoCredential credential; 
	      credential = MongoCredential.createCredential("sampleUser", "tweets", 
	         "password".toCharArray()); 
	      System.out.println("Connected to the database successfully");  
	      
	      // Accessing the database 
	      database = mongo.getDatabase("tweets"); 
	      
	   // Retrieving a collection
	      collection = database.getCollection("streaming_test2"); 
	      
		}
		return database;
	}
	
	
	public static synchronized void closeConnection() throws IOException {
		mongo.close();
        mongo = null;
    }
	
	
	public static void IncrementHashCount(Long timeStamp,String hashTag,Number count) {
		makeConnection();
		Date time =  new Date(timeStamp);
		

		BasicDBObject dateQuery = new BasicDBObject();
		
		String pattern = "ddMMyyyyHH";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		dateQuery.put("name", simpleDateFormat.format(time) );
	    	    
		 Bson update =  new Document("$set",
                 new Document()
                       .append("name", simpleDateFormat.format(time)));
	    
	    UpdateOptions options = new UpdateOptions().upsert(true);
	    UpdateResult res = collection.updateOne(dateQuery, update,options);
	    System.out.printf("====== Insert date to db: %s\n",res);
	    
	    //Updating existing hashtag entry
	    BasicDBObject query = new BasicDBObject();
	    query.put("name", simpleDateFormat.format(time) );
	    query.put("hashktags.hashtags", hashTag);
	    
	    BasicDBObject incValue = new BasicDBObject("hashktags.$.count", count); 
	    BasicDBObject intModifier = new BasicDBObject("$inc", incValue); 
	    
	    res = collection.updateOne(query, intModifier);
	    System.out.printf("====== Update to db: %s\n",res);
	    
	    if (res.getModifiedCount() == 0) {
	    	//insert new entry
		    Bson insertNewHash =  new Document("$push",
	                new Document()
	                      .append("hashktags", new Document().append("hashtags", hashTag).append("count", count)));
		    
		   
		    res = collection.updateOne(dateQuery, insertNewHash);
		    System.out.printf("====== insert new hashtag to db: %s\n",res);
	    }
	    
			
	}
	
}



