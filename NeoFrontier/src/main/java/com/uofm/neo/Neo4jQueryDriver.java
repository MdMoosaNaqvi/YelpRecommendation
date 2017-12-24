package com.uofm.neo;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;

public class Neo4jQueryDriver {
	
	public List<String> getUserRatedBusinessList(String userId) {
		Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
		Session session = driver.session();
		List<String> businessIdList = new ArrayList<String>();
		StatementResult result = session.run( "MATCH (u:YelpUser{user_id :{user_id}})-[:HAS_REVIEWED]->(yelpBusiness:YelpBusiness) " +
		                                      "RETURN yelpBusiness",
		        parameters( "user_id", userId ) );
		while ( result.hasNext() )
		{
		    Record record = result.next();
		    Value yelpBusiness = record.get("yelpBusiness");
		    businessIdList.add(yelpBusiness.get("business_id").asString());
		}

		session.close();
		driver.close();
		return businessIdList;
	}
	
	public Set<String> getAllUserList(List<String> businessIdList) {
		Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
		Session session = driver.session();
		Set<String> userIdSet = new HashSet<String>();
		StatementResult result = session.run( "MATCH (u:YelpUser)-[:HAS_REVIEWED]->(yelpBusiness:YelpBusiness) " +
												"WHERE yelpBusiness.business_id IN {businessIdList} " + 
		                                      "RETURN u",
		        parameters( "businessIdList", businessIdList ) );
		while ( result.hasNext() )
		{
		    Record record = result.next();
		    Value yelpUser = record.get("u");
		    
		    userIdSet.add(yelpUser.get("user_id").asString());
		}

		session.close();
		driver.close();
		return userIdSet;
	}
	
	public Map<String, Double> listAllBusinessOfUser(String userId) {
		Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
		Session session = driver.session();
		StatementResult result = session.run( "MATCH (yelpUser:YelpUser{user_id:{user_id}})-[review:HAS_REVIEWED]->(yelpBusiness:YelpBusiness) " +
		                                      "RETURN yelpUser, review, yelpBusiness",
		        parameters( "user_id", userId ) );
		Map<String, Double> businessRatingMap = new HashMap<String, Double>();
		
		while ( result.hasNext() )
		{
			 Record record = result.next();
			 Value review = record.get("review");
			 Value user = record.get("yelpUser");
			 double rating = review.get("stars").asFloat();
			 String businessId = review.get("business_id").asString();
		    
			 double averageStars = user.get("average_stars").asDouble();
			 double normalizedRating = rating - averageStars;
			 businessRatingMap.put(businessId, normalizedRating);
		}
		
		session.close();
		driver.close();
		return businessRatingMap;
	}
	public double calculateCosineDistance(Map<String, Double> businessRatingMap1, Map<String, Double> businessRatingMap2) {
		 double dotProduct = 0.0;
		    double normA = 0.0;
		    double normB = 0.0;
		    Iterator<Entry<String, Double>> map1Iterator = businessRatingMap1.entrySet().iterator();
		    while(map1Iterator.hasNext()) {
		    	Entry<String, Double> entry = map1Iterator.next();
		    	if(businessRatingMap2.containsKey(entry.getKey())) {
		    		dotProduct += entry.getValue() * businessRatingMap2.get(entry.getKey());
		    		businessRatingMap2.remove(entry.getKey());
		    	} else {
		    		normA += Math.pow(entry.getValue(), 2);
		    	}
		    	map1Iterator.remove();
		    }
		    if(!businessRatingMap2.isEmpty()) {
		    	Iterator<Entry<String, Double>> map2Iterator = businessRatingMap2.entrySet().iterator();
		    	while(map2Iterator.hasNext()) {
		    		Entry<String, Double> entry = map2Iterator.next();
		    		normB += Math.pow(entry.getValue(), 2);
		    		map2Iterator.remove();
		    	}
		    }
		    
		    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}
	
	public void printBusiness(Set<String> userIdList, Map<String, Double> presentUserBusinessRatingMap, String userId) {
		Set<String> businessNameSet = new HashSet<String>();
		for(String similarUserId : userIdList) {
			Map<String, Double> userBusinessRatingMap = listAllBusinessOfUser(similarUserId);
			Double cosineDistance = calculateCosineDistance(presentUserBusinessRatingMap, userBusinessRatingMap);
			// cosine distance defines user similarity i.e. higher value means both users have rated same items similarly
			if(cosineDistance > .5) {
				Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
				Session session = driver.session();
		 		StatementResult result = session.run( "MATCH (user:YelpUser)-[hr:HAS_REVIEWED]->(yb:YelpBusiness) WHERE user.user_id = {user_id1} " +
		 				"AND NOT (user:YelpUser{user_id: {user_id2}})-[hr:HAS_REVIEWED]->(yelpBusinessb:YelpBusiness) " +
		 				"return yelpBusiness",
				        parameters( "user_id1", similarUserId, "user_id2", userId));
		 		while ( result.hasNext() )
				{
				    Record record = result.next();
				    Value yelpBusiness = record.get("yelpBusiness");
				    
				    businessNameSet.add(yelpBusiness.get("name").asString());
				}
			}
		}
		System.out.println("____________Recommended Business__________________");
		for(String businessName : businessNameSet) {
			System.out.println(businessName);
		}
	}
public static void main(String[] args) {
	Neo4jQueryDriver neo4jQueryDriver = new Neo4jQueryDriver();
	String userId = "kyhxBq6x_Pl1GEgq0g-CFA"; 
	List<String> businessIdList = neo4jQueryDriver.getUserRatedBusinessList(userId);
	Map<String, Double> presentUserBusinessRatingMap = neo4jQueryDriver.listAllBusinessOfUser(userId);
	Set<String> userIdList = neo4jQueryDriver.getAllUserList(businessIdList);
	neo4jQueryDriver.printBusiness(userIdList, presentUserBusinessRatingMap, userId);
}
}
