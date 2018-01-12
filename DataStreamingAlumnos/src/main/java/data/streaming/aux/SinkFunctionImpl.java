package data.streaming.aux;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import data.streaming.dto.TweetDTO;
import data.streaming.test.MongoDB;

public class SinkFunctionImpl implements SinkFunction<TweetDTO> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void invoke(TweetDTO twitter) throws Exception {
		if (twitter.getCreatedAt() != null) {
			for (int i = 0; i < data.streaming.test.TestFlinkKafkaConsumer.keywords.length; i++) {
				int index = twitter.getText().indexOf(data.streaming.test.TestFlinkKafkaConsumer.keywords[i]);
				if (index != -1) {
					org.bson.Document validTwitter = new org.bson.Document();
					validTwitter.put("keyword", data.streaming.test.TestFlinkKafkaConsumer.keywords[i]);
					validTwitter.put("date", twitter.getCreatedDate());
					data.streaming.test.TestFlinkKafkaConsumer.validTwitters.add(validTwitter);
					
					if (data.streaming.test.TestFlinkKafkaConsumer.validTwitters.size() >= 5000) {
						try {
							data.streaming.test.TestFlinkKafkaConsumer.MDB.insertManyTwitter(data.streaming.test.TestFlinkKafkaConsumer.validTwitters);
							System.out.println("Insert " + data.streaming.test.TestFlinkKafkaConsumer.validTwitters.size() + " news twitter");
							data.streaming.test.TestFlinkKafkaConsumer.validTwitters.clear();							
						} catch (Exception name) {
							System.out.println(name.getMessage());
						}

					}
					
				}
			}
		}
	}
}
