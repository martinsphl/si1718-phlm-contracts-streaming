package data.streaming.test;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import groovyjarjarantlr.debug.ParserTokenAdapter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.math3.stat.Frequency;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDB {

	private static MongoClientURI uri;
	private static MongoClient client;
	private static MongoDatabase db;
	private Map<String, Integer> sumFrequency = null;

	public void closeClient() {
		client.close();
	}

	public MongoDB() {
		uri = new MongoClientURI("mongodb://admin:1234@ds251435.mlab.com:51435/si1718-phlm-contracts");
		client = new MongoClient(uri);
		db = client.getDatabase(uri.getDatabase());
	}

	public String[] getKeywords() {
		MongoCollection<org.bson.Document> contractsCollection = db.getCollection("contractsKeywords");
		MongoCursor<org.bson.Document> cursor = contractsCollection.find().iterator();

		List<String> lsKeywords = new ArrayList<String>();

		while (cursor.hasNext()) {
			String[] aux = cursor.next().toString().split("keywords=");
			aux[1] = aux[1].replace("]}}", "").replace("[", "");
			aux = aux[1].split(",");
			for (int i = 0; i < aux.length; i++)
				lsKeywords.add(aux[i].trim());
		}
		cursor.close();

		String[] keywords = new String[lsKeywords.size()];
		for (int i = 0; i < keywords.length; i++) {
			keywords[i] = lsKeywords.get(i);
		}
		return keywords;
	}

	public void insertManyTwitter(ArrayList<org.bson.Document> seedTwitterValid) {
		MongoCollection<org.bson.Document> twitter = db.getCollection("twitterKeywords4");
		twitter.insertMany(seedTwitterValid);
	}

	public Map<String, Integer> countingTwitterKeywords0(Map<String, Integer> twitterKeyword, String[] keywords) {

		MongoCollection<org.bson.Document> twitterKeywordsCollection = db.getCollection("twitterKeywords0");
		BasicDBObject query = new BasicDBObject();

		List<String> dateQuery = new ArrayList<String>();
		MongoCursor<String> cursorDate;

		cursorDate = twitterKeywordsCollection.distinct("Date", String.class).iterator();
		while (cursorDate.hasNext()) {
			dateQuery.add(cursorDate.next().toString());
		}

		cursorDate = twitterKeywordsCollection.distinct("date", String.class).iterator();
		while (cursorDate.hasNext()) {
			dateQuery.add(cursorDate.next().toString());
		}

		for (int i = 0; i < dateQuery.size(); i++) {
			query.put("date", dateQuery.get(i));
			MongoCursor<Document> cursor = twitterKeywordsCollection.find(query).iterator();

			String date = dateQuery.get(i);
			while (cursor.hasNext()) {
				try {
					String keyword = cursor.next().get("keyword").toString();
					if (sumFrequency.get(keyword) != null)
						sumFrequency.put(keyword, sumFrequency.get(keyword) + 1);
					else
						sumFrequency.put(keyword, 1);
				} catch (Exception e) {
					break;
				}
			}

			for (int j = 0; j < keywords.length; j++) {
				twitterKeyword.put(date + "." + keywords[j], sumFrequency.get(keywords[j]));
				if (date.indexOf("Dec") != -1)
					twitterKeyword.put("Dec." + keywords[j],
							twitterKeyword.get("Dec." + keywords[j]) + sumFrequency.get(keywords[j]));
				else
					twitterKeyword.put("Jan." + keywords[j],
							twitterKeyword.get("Jan." + keywords[j]) + sumFrequency.get(keywords[j]));
			}
			cursor.close();
		}
		return twitterKeyword;
	}

	public Map<String, Integer> countingKeywords(Map<String, Integer> twitterKeyword, String nmCollection,
			String[] keywords) {

		MongoCollection<org.bson.Document> twitterKeywordsCollection = db.getCollection(nmCollection);
		BasicDBObject query = new BasicDBObject();

		List<String> dateQuery = new ArrayList<String>();
		/*
		 * MongoCursor<String> cursorDate;
		 * 
		 * 
		 * cursorDate = twitterKeywordsCollection.distinct("date",
		 * String.class).iterator();
		 * 
		 * while (cursorDate.hasNext()) { dateQuery.add(cursorDate.next().toString()); }
		 */

		// for (int i=0; i < keywords.length; i++) {
		// dateQuery.add(prefix + "." + keywords[i]);
		// }

		// for (int i = 0; i < dateQuery.size(); i++) {

		Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		Date dateSystem = calendar.getTime();
		String date = dateSystem.toString().substring(0, 11);
		date = date + calendar.get(Calendar.YEAR);

		query.put("date", date);
		MongoCursor<Document> cursor = twitterKeywordsCollection.find(query).iterator();

		// String date = dateQuery.get(i);
		while (cursor.hasNext()) {
			try {
				String keyword = cursor.next().get("keyword").toString();
				if (sumFrequency.get(keyword) != null)
					sumFrequency.put(keyword, sumFrequency.get(keyword) + 1);
				else
					sumFrequency.put(keyword, 1);
			} catch (Exception e) {
				continue;
			}
		}

		for (int j = 0; j < keywords.length; j++) {
			twitterKeyword.put(date + "." + keywords[j], sumFrequency.get(keywords[j]));
			if (date.indexOf("Dec") != -1)
				twitterKeyword.put("Dec." + keywords[j],
						twitterKeyword.get("Dec." + keywords[j]) + sumFrequency.get(keywords[j]));
			else
				twitterKeyword.put("Jan." + keywords[j],
						twitterKeyword.get("Jan." + keywords[j]) + sumFrequency.get(keywords[j]));
		}
		cursor.close();
		// }
		return twitterKeyword;
	}

	public void keywordsStatistics() {
		Map<String, Integer> twitterKeyword = new HashMap<String, Integer>();
		sumFrequency = new HashMap<String, Integer>();

		String[] keywords = getKeywords();
		for (int i = 0; i < keywords.length; i++) {
			twitterKeyword.put("Dec." + keywords[i], 0);
			twitterKeyword.put("Jan." + keywords[i], 0);
		}

		twitterKeyword = countingKeywords(twitterKeyword, "twitterKeywords4", keywords);
		// for (int i = 2; i < 5; i++)
		// twitterKeyword = countingKeywords(twitterKeyword, "twitterKeywords" + i,
		// keywords);

		// twitterKeyword = countingTwitterKeywords0(twitterKeyword, keywords);

		// Foi alterado recentimente
		//recordKeywordsStatistics(twitterKeyword);
	}

	public void recordKeywordsStatistics(Map<String, Integer> twitterKeyword) {
		/*
		 * ArrayList<org.bson.Document> keywordsStatistics = new
		 * ArrayList<org.bson.Document>(); for (String key : twitterKeyword.keySet()) {
		 * org.bson.Document keywordStatistic = new org.bson.Document();
		 * keywordStatistic.put("key", key); keywordStatistic.put("frequency",
		 * twitterKeyword.get(key)); keywordsStatistics.add(keywordStatistic); }
		 * 
		 * // keywordsStatisticsCollection.drop();
		 * keywordsStatisticsCollection.insertMany(keywordsStatistics);
		 * System.out.println("Estatisticas atualizadas");
		 */
		MongoCollection<org.bson.Document> keywordsStatisticsCollection = db
				.getCollection("Teste_twitterKeywordsStatistics");
		MongoCollection<org.bson.Document> kdStatisticsCollection = db
				.getCollection("twitterKeywordsStatistics");
		org.bson.Document newStatisticsKW = new org.bson.Document();
		for (String key : twitterKeyword.keySet()) {
			BasicDBObject searchQuery = new BasicDBObject().append("key", key);
			Object freqBefore = kdStatisticsCollection.find(searchQuery).first();
			System.out.println(freqBefore);
			newStatisticsKW.put("key", key); 
			newStatisticsKW.put("frequency",twitterKeyword.get(key));
			searchQuery = new BasicDBObject().append("key", key);
			keywordsStatisticsCollection.updateMany(searchQuery, newStatisticsKW);
		}
	}

	public Map<String, String> recordKeywordsContracts() {
		MongoCollection<org.bson.Document> contractsCollection = db.getCollection("newContracts");
		MongoCursor<org.bson.Document> cursor = contractsCollection.find().limit(600).iterator();
		// MongoCursor<org.bson.Document> cursor =
		// contractsCollection.find().iterator();
		Map<String, String> keywordsContracts = new HashMap<String, String>();
		while (cursor.hasNext()) {
			try {
				String key, value = "";
				key = cursor.next().get("idContract").toString();
				if (key.length() == 0)
					continue;
				value = cursor.next().get("keywords").toString();
				keywordsContracts.put(key, value);
			} catch (Exception e) {
				continue;
			}
		}

		return keywordsContracts;
	}

	public void recordClassificationContracts(ArrayList<classificationContracts> cfcContract) {
		MongoCollection<org.bson.Document> ratingContractsCollection = db.getCollection("ratingContracts");
		ratingContractsCollection.drop();
		ArrayList<org.bson.Document> classificationContracts = new ArrayList<org.bson.Document>();
		for (int i = 0; i < cfcContract.size(); i++) {
			org.bson.Document twoContracts = new org.bson.Document();
			twoContracts.put("contract1", cfcContract.get(i).getContract1());
			twoContracts.put("contract2", cfcContract.get(i).getContract2());
			twoContracts.put("rating", cfcContract.get(i).getR());
			classificationContracts.add(twoContracts);

			if ((i + 1) % 100000 == 0) {
				ratingContractsCollection.insertMany(classificationContracts);
				classificationContracts.clear();
			}
		}

		if (classificationContracts.size() > 0) {
			ratingContractsCollection.insertMany(classificationContracts);
			classificationContracts.clear();
		}
	}

	public void recordRecommendationContracts(ArrayList<org.bson.Document> recommendationContracts) {
		MongoCollection<org.bson.Document> ratingContractsCollection = db.getCollection("recommendationContracts");
		ratingContractsCollection.drop();
		ratingContractsCollection.insertMany(recommendationContracts);
	}
}
