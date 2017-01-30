package consumer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.spark.MongoSpark;

/**
 * Spark Kafka consumer - listen to customer, account and transaction topics
 * aggregates 10 records from Kafka, those records will be scanned with Spark on suspicious records
 * records will be written to MongoDB in micro batches of 10 records  
 * 
 * @see https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 * @see https://docs.mongodb.com/spark-connector/v1.1/java-api/
 */
public class SparkConsumer implements Serializable, Runnable {

	List<Document> transactionDocuments = new ArrayList<Document>();
	List<Document> customerDocuments = new ArrayList<Document>();
	List<Document> accountDocuments = new ArrayList<Document>();

	/*
	 * Spark executer listen to local Kafka topics
	 */
	public void execute() throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		/*
		 * Make this configurable 
		 */
		kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList("Customer", "Transaction", "Account");

		/*
		 * Kafka stream listen to topics
		 */
		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		/*
		 * stream each record to business logic
		 */
		stream.foreachRDD(rdd -> {
			rdd.foreach(record -> saveRecord(record));
		});

		jssc.start();
		jssc.awaitTermination();
	}

	/**
	 * based on topic do some magic and save to collection producer is producing JSONArrays
	 * 
	 * @param record
	 */
	@SuppressWarnings("unchecked")
	private void saveRecord(ConsumerRecord<String, String> record) {
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = null;
		try {

			switch (record.topic()) {
			case "Customer":
				/*
				 * transform record value to JSON and finally to Document perform magic with records TODO write 10 records in parallel to mongoDB
				 */
				jsonObject = (JSONObject) parser.parse(record.value());
				if (customerDocuments.size() == 10) {
					// System.out.println("==Saving 10 records to MongoDB==");
					/**
					 * ETL Logic / Tasks happen here
					 */
					MongoSpark.save(jssc.sparkContext().parallelize(customerDocuments));
					customerDocuments = new ArrayList<Document>();
				}
				customerDocuments.add(Document.parse(jsonObject.toString()));
				break;
			case "Transaction":
				/*
				 * transform record value to JSON and finally to Document scan transactions for suspicious amounts and inform user write 10 records in parallel to mongoDB
				 */
				jsonObject = (JSONObject) parser.parse(record.value());
				try {
					/**
					 * ETL Logic / Tasks happen here
					 * 
					 * scan transactions for suspicious or threshold exceeding transaction
					 */
					BigDecimal amount = parseCurrency(jsonObject.get("amount").toString(), Locale.US);
					if (amount.intValue() > 990){
						System.out.println("found suspicious high transaction amount " + amount + " on transcation id " + jsonObject.get("id"));
						jsonObject.put("suspicious", "true");
					}
				} catch (java.text.ParseException e) {
					e.printStackTrace();
				}
				transactionDocuments.add(Document.parse(jsonObject.toString()));

				if (transactionDocuments.size() == 10) {
					// System.out.println("==Saving 10 records to MongoDB==");
					MongoSpark.save(jssc.sparkContext().parallelize(transactionDocuments));
					transactionDocuments = new ArrayList<Document>();
				}
				break;
			case "Account":
				/*
				 * TODO
				 */
				break;

			default:
				break;
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println(record.value());
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * extract amount on given currency
	 * 
	 * @param amount
	 *            string including $€ signs
	 * @param locale
	 *            depending on locale the currency will converted to BigDecimal
	 * @return
	 * @throws java.text.ParseException
	 */
	private BigDecimal parseCurrency(final String amount, final Locale locale) throws java.text.ParseException {
		final NumberFormat format = NumberFormat.getNumberInstance(locale);
		if (format instanceof DecimalFormat) {
			((DecimalFormat) format).setParseBigDecimal(true);
		}
		return (BigDecimal) format.parse(amount.replaceAll("[^\\d.,]", ""));
	}

	private static JavaStreamingContext jssc = null;
	private static final long serialVersionUID = 985258810331654697L;

	
	/*
	 * local spark listener writes to local MongoDB
	 */
	@Override
	public void run() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkBuzzwords").set("spark.mongodb.output.uri",
				"mongodb://127.0.0.1/spktest.transactions");
		jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		try {
			(new SparkConsumer()).execute();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
}
