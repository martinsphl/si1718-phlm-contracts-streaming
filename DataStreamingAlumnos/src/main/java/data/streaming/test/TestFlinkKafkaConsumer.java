package data.streaming.test;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import data.streaming.aux.SinkFunctionImpl;
import data.streaming.dto.TweetDTO;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class TestFlinkKafkaConsumer {

	public static MongoDB MDB = new MongoDB();
	public static String[] keywords = MDB.getKeywords();
	public static ArrayList<org.bson.Document> validTwitters = new ArrayList<org.bson.Document>();

	public static void main(String... args) throws Exception {
		
		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = LoggingFactory.getCloudKarafkaCredentials();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(
				props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(), props));

		SinkFunction<TweetDTO> sinkFunction = new SinkFunctionImpl();
		stream.filter(x -> Utils.isValid(x)).map(x -> Utils.createTweetDTO(x)).addSink(sinkFunction);
		
		// execute program
		env.execute("Twitter Streaming Consumer");

	}
}
