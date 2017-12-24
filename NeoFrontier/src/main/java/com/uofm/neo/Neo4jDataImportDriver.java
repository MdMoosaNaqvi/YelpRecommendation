package com.uofm.neo;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Date;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.DatabaseException;

public class Neo4jDataImportDriver {
	public static void main(String []args) {
		new Neo4jDataImportDriver().readFolder();
	}
	
	public void readFolder() {
		Date startTime = new Date();
		String folderPath = "C:/xampp/php/output/split_output_review";
		File file = new File(folderPath); 
		String[] fileList = file.list();
		String completeFilePath = "";
		int trainingSetSize = (int)(fileList.length * .8);
		System.out.println("TrainingSetSize " + trainingSetSize);
		for(int index = 0; index<trainingSetSize; index++) {
			String filePath = fileList[index];
			completeFilePath = folderPath + "/" + filePath;
			System.out.println("Current file path " + completeFilePath + " Index " + index);
			loadCsvForReview(completeFilePath);
		}
		Date stopTime = new Date();
		long totalTime = (stopTime.getTime() - startTime.getTime())/60000;
		System.out.println("Finished review node & relationship creation at " + stopTime.toString()+". Total time taken " + totalTime + " minutes");
		
	}
	
	public void loadCsvForBusiness(String filePath) {
		Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
		Session session = driver.session();
		try {
			session.run("LOAD CSV WITH HEADERS FROM 'file:///" + filePath +"' AS line Create (a:YelpBusiness{business_id:line.business_id, name:line.name, neighborhood:line.neighborhood, address:	line.address, city : line.city, state : line.state, postal_code:toInteger(line.postal_code), latitude:toFloat(line.latitude), longitude:toFloat(line.longitude), stars:toFloat(line.stars), review_count: toInteger(line.review_count), is_open: toInteger(line.is_open), categories : split(substring(line.categories, 0, size(line.categories)-1), ',')})" );
			session.close();
			driver.close();
		}catch(DatabaseException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
		}

	}

	public void loadCsvForUser(String filePath) {
		Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
		Session session = driver.session();
		try {
			session.run("LOAD CSV WITH HEADERS FROM 'file:///" + filePath +"' AS line Create (a:YelpUser{user_id:line.user_id, name:line.name, review_count:toInteger(line.review_count), yelping_since:	line.yelping_since, friends : split(substring(line.friends, 0, size(line.friends)-1), ','), useful:toInteger(line.useful), funny:toInteger(line.funny), cool:toInteger(line.cool), fans:toInteger(line.fans), elite: split(substring(line.elite, 0, size(line.elite)), ','), average_stars: toFloat(line.average_stars), compliment_hot:toInteger(line.compliment_hot), compliment_cool:toInteger(line.compliment_cool), compliment_funny:toInteger(line.compliment_funny), compliment_cute:toInteger(line.compliment_cute)})" );
			session.close();
			driver.close();
		}catch(DatabaseException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
		}

	}
	
	public void loadCsvForReview(String filePath) {
		Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic( "neo4j", "moosa" ) );
		Session session = driver.session();
		try {
			session.run("LOAD CSV WITH HEADERS FROM 'file:///" + filePath +"' AS review CREATE (r:Review) SET r = review WITH r MATCH (u:YelpUser {user_id: r.user_id}) MATCH (b:YelpBusiness {business_id: r.business_id}) MERGE (u)-[hr:HAS_REVIEWED{review_id:r.review_id,user_id:r.user_id, business_id:r.business_id, stars:toFloat(r.stars),date:r.date,useful:toInteger(r.useful),funny:toInteger(r.funny)}]->(b)");
			session.close();
			driver.close();
		}catch(DatabaseException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
		}

	}
	
	public void replaceInFile(String fileName) {
		try {
			File file = new File(fileName);
			File tempFile = File.createTempFile("buffer", ".tmp");
			FileWriter fw = new FileWriter(tempFile);
			
			Reader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			
			while(br.ready()) {
				String modifiedLine = br.readLine().replaceAll("\\\\", "/");
				fw.write(modifiedLine + "\n");
			}
			
			fw.close();
			br.close();
			fr.close();
			
			// Finally replace the original file.
			String newFileName = fileName.substring(0, fileName.lastIndexOf("_")) + "_" + new Date().getTime() + ".csv";

			tempFile.renameTo(new File(newFileName));
			file.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	
}
