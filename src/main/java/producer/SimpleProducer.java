package producer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * TODO: multi thread approach - simulate multi producers
 */
public class SimpleProducer {

	public static void main(String[] args) throws Exception {
		/*
		 * This a demo producer - could be any tool on any machine like a mainframe - CDC approach
		 *  
		 * Setup Kafka properties and topics
		 * 
		 * TODO Make this configurable
		 */
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		/*
		 * Define new Kafka producer
		 */
		@SuppressWarnings("resource")
		Producer<String, String> producer = new KafkaProducer<>(props);

		/*
		 * parallel generation of JSON messages on Transaction topic
		 * 
		 * this could also include business logic, projection, aggregation, etc.
		 */
		Thread transactionThread = new Thread(new Runnable() {
			public void run() {
				String topic = "Transaction";
				List<JSONObject> transactions = new ArrayList<JSONObject>();
				initJSONData("ETLSpark_Transactions.json", transactions);
				for (int i = 0; i < transactions.size(); i++) {
					if (i % 200 == 0) {
						System.out.println("200 Transaction Messages procuded");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), transactions.get(i).toString()));
				}
			}
		});

		/*
		 * parallel generation of customer topic messages 
		 */
		Thread customerThread = new Thread(new Runnable() {
			public void run() {
				String topic = "Customer";
				List<JSONObject> customer = new ArrayList<JSONObject>();
				initJSONData("ETLSpark_Customer.json", customer);
				for (int i = 0; i < customer.size(); i++) {
					if (i % 200 == 0) {
						System.out.println("200 Customer Messages procuded");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), customer.get(i).toString()));
				}
			}
		});

		/*
		 * parallel generation of messages (variable produceMessages)
		 * 
		 * generated messages are based on mockaroo api
		 */
		int produceMessages = 100000;
		Thread accountThread = new Thread(new Runnable() {
			public void run() {
				String topic = "Account";
				for (int i = 0; i < produceMessages; i++) {
//					System.out.println("Account procuded");
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), getRandomTransactionJSON(i)));
				}
			}
		});

		transactionThread.start();
		customerThread.start();
		accountThread.start();

	}

	/**
	 * helper method to generate random transactions
	 * 
	 * @param i
	 *            - int counter
	 * @return random transaction message
	 */
	public static String getRandomTransactionJSON(int i) {
		return "{" + "id: " + i + ", " + "ts: {\"$date\": \"" + getRandomTimeStamp() + "\"}, " + "balance: " + ((int) (Math.random() * 5000)) + ", "
				+ "desc: \"" + "DescriptionXXX" + "\", " + "date: {\"$date\": \"" + getRandomTimeStamp() + "\"}, " + "amount: " + ((int) (Math.random() * 1000))
				+ "}";
	}

	// TODO: JSON->BSON format must be ISO-8601 - hard-coding for moment as Java
	// API
	public static String getRandomTimeStamp() {
		return "2014-12-12T10:39:40Z";
	}

	/*
	 * load Data from JSON Files to given list
	 */
	private static void initJSONData(String filename, List<JSONObject> list) {

		try {
			// init transactions list
			String transactionsContent = FileUtils.readFileToString(new File(filename), "UTF-8");

			JSONArray jArray = (JSONArray) new JSONTokener(transactionsContent).nextValue();
			for (int i = 0; i < jArray.length(); i++) {
				list.add((JSONObject) jArray.get(i));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}