package producer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * TODO: multi thread approach - simulate multi producers
 */
public class SimpleProducer {

	public static void main(String[] args) throws Exception {
		/*
		 * Setup Kafka properties
		 * 
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
		Producer<String, String> producer = new KafkaProducer<>(props);

		/**
		 * parallel generation of messages (variable produceMessages) 
		 * 
		 * generated messages are based on mockaroo api and producing array of JSON entries 
		 */
		int produceMessages = 100000;
		Thread transactionThread = new Thread(new Runnable() {
			public void run() {
				String topic = "Transaction";
				for (int i = 0; i < produceMessages; i++) {
					System.out.println("Transaction procuded");
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i),
							getMockarooData("ETLSpark_Transactions", 5)));
				}
			}
		});

		Thread customerThread = new Thread(new Runnable() {
			public void run() {
				String topic = "Customer";
				for (int i = 0; i < produceMessages; i++) {
					System.out.println("Customer procuded");
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i),
							getMockarooData("ETLSpark_Customer", 5)));
				}
			}
		});

		Thread accountThread = new Thread(new Runnable() {
			public void run() {
				String topic = "Account";
				for (int i = 0; i < produceMessages; i++) {
					System.out.println("Account procuded");
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i),
							getRandomTransactionJSON(i)));
				}
			}
		});

		transactionThread.start();
		customerThread.start();
//		accountThread.start();

	}

	/**
	 * helper method to generate random transactions
	 * 
	 * @param i
	 *            - int counter
	 * @return random transaction message
	 */
	public static String getRandomTransactionJSON(int i) {
		return "{" + "id: " + i + ", " + "ts: {\"$date\": \"" + getRandomTimeStamp() + "\"}, " + "balance: "
				+ ((int) (Math.random() * 5000)) + ", " + "desc: \"" + "DescriptionXXX" + "\", "
				+ "date: {\"$date\": \"" + getRandomTimeStamp() + "\"}, " + "amount: " + ((int) (Math.random() * 1000))
				+ "}";
	}

	// TODO: JSON->BSON format must be ISO-8601 - hard-coding for moment as Java
	// API
	public static String getRandomTimeStamp() {
		return "2014-12-12T10:39:40Z";
	}

	/**
	 * connect to mockaroo api (thanks to Benjamin Lorenz) and generate based on defined templates sample records 
	 * 
	 * @param schema - predefined schmea (currently only ETLSpark_Customer or ETLSpark_Transaction)
	 * @param count - amount of JSON inside a generated array [{JSON1}, {JSON2}, ...]
	 * @return
	 */
	public static String getMockarooData(String schema, int count) {
		URL url;
		String input = "";
		try {
			//api key 45839d40
			//http://www.mockaroo.com/api/generate.json?key=45839d40&schema=tim_test&count=5
			url = new URL("http://www.mockaroo.com/api/generate.json?key=45839d40&schema="+schema+"&count="+count);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			
			input = IOUtils.toString(conn.getInputStream(), "UTF-8");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return input;

	}

}