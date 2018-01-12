package data.streaming.test;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.Map;

import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.joda.time.DateTime;

import data.streaming.dto.KeywordDTO;
import data.streaming.utils.Utils;

public class TestBatch {
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public void executeBatch() {
		final Runnable action = new Runnable() {
			public void run() {
				//try {
					DateTime dt = new DateTime();
					System.out.println(dt);
					int hourUpdate = dt.getHourOfDay();
					
					MongoDB MDB = new MongoDB();
					MDB.keywordsStatistics();
					/*
					if ( (hourUpdate % 2) == 0) {
						Map<String , String> keywordsContracts = MDB.recordKeywordsContracts();
						CreateExportRatingContracts ratingContracts = new CreateExportRatingContracts(
								keywordsContracts);
						ratingContracts.createRatingContracts();

						Set<KeywordDTO> set = Utils.getKeywords();
						ItemRecommender irec = Utils.getRecommender(set);
						Utils.saveModel(irec, set);
					}
					*/

				//} catch (IOException e) {
				//	e.printStackTrace();
				//} catch (RecommenderBuildException e) {
				//	e.printStackTrace();}

			}
		};

		final ScheduledFuture<?> handle = scheduler.scheduleAtFixedRate(action, 10, 60, SECONDS);

		scheduler.schedule(new Runnable() {
			public void run() {
				handle.cancel(true);
			}
		}, 60 * 60, SECONDS);
	}

	public static void main(String... args) {
		new TestBatch().executeBatch();
	}
}