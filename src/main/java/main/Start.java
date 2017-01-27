package main;

import consumer.SparkConsumer;
import producer.SimpleProducer;

public class Start {
	public static void main(String[] args) {
		try {
			/*
			 * Start consumer first - implemented as thread for parallel execution
			 */
			System.out.println("Start Spark consumer");
			
			Thread sc = new Thread(new SparkConsumer());
			sc.start();
			
			System.out.println("Spark consumer successul started");
			/*
			 * sleep
			 */
			Thread.sleep(200);

			/*
			 * Start producer - in loop to sequentially run the same data again
			 */
			System.out.println("Producer start");
			for(int i = 0; i < 1; i++)
				SimpleProducer.main(args);
			
			System.out.println("Producer done");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
