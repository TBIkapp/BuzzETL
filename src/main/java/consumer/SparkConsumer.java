package consumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONTokener;

import com.mongodb.spark.MongoSpark;

/**
 * 
 * 
 * @see https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 * @see https://docs.mongodb.com/spark-connector/v1.1/java-api/
 */
public class SparkConsumer implements Serializable {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkBuzzwords")
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/spktest.transactions");
		jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		(new SparkConsumer()).execute();
	}

	public void execute() throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList("Customer", "Transaction", "Account");

		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		stream.foreachRDD(rdd -> {
			rdd.foreach(record -> saveRecord(record));
		});

		jssc.start();
		jssc.awaitTermination();
	}

	/**
	 * based on topic do some magic and save to collection
	 * producer is producing JSONArrays 
	 * @param record
	 */
	private void saveRecord(ConsumerRecord<String, String> record) {

		switch (record.topic()) {
		case "Customer":
			/*
			 * TODO
			 */
			break;
		case "Transaction":
			List<Document> documents = new ArrayList<Document>();
			JSONArray jArray = (JSONArray) new JSONTokener(record.value()).nextValue();
			for (int i = 0; i < jArray.length(); i++) {
				documents.add(Document.parse(jArray.get(i).toString()));
			}

			System.out.println("==Saving record '" + record.key() + "' to MongoDB==");
			MongoSpark.save(jssc.sparkContext().parallelize(documents));

			break;
		case "Account":
			/*
			 * TODO
			 */
			break;

		default:
			break;
		}
	}

	private static JavaStreamingContext jssc = null;
	private static final long serialVersionUID = 985258810331654697L;
}
