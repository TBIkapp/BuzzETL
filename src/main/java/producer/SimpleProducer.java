package producer;

import java.util.Properties;

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
		 * generate and send 100 messages based on a single topic
		 * this could be also used for a list of topics or continuous message generator  
		 */
		String topic = "Transaction";
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), getRandomTransactionJSON(i)));
		}
		producer.close();
	}
	
	/**
	 * helper method to generate random transactions
	 * @param i - int counter
	 * @return random transaction message
	 */
	public static String getRandomTransactionJSON(int i) {
		return "{" +
					"id: " + i + ", " + 
					"ts: {\"$date\": \"" + getRandomTimeStamp() + "\"}, " + 
					"balance: " + ((int) (Math.random() * 5000)) + ", " + 
					"desc: \"" + "DescriptionXXX" + "\", " + 
					"date: {\"$date\": \"" + getRandomTimeStamp() + "\"}, " + 
					"amount: " + ((int) (Math.random() * 1000)) +
				"}";
	}

	// TODO: JSON->BSON format must be  ISO-8601 - hard-coding for moment as Java API
	public static String getRandomTimeStamp() {
		return "2014-12-12T10:39:40Z";
	}
}