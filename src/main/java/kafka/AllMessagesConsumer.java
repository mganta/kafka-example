package kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class AllMessagesConsumer implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private Properties properties;

	public AllMessagesConsumer(Properties properties)  {
		this.properties = properties;
	}

	public void run() {
			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		    consumer.subscribe(Arrays.asList(properties.getProperty("consumer_topic")));
			while (!closed.get()) {
				
				ConsumerRecords<String, String> records = consumer.poll(10000);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Received message: (" + record.key()
							+ ", " + record.value() + ") at offset "
							+ record.offset());
					System.out.println("Received on Partition: "
							+ record.partition() + " and topic: "
							+ record.topic());
				}
				System.out.println("Consumer batch done...");
				consumer.commitSync();
			}
			consumer.close();
	}

	// Shutdown hook 
	public void shutdown() {
		closed.set(true);
	}

	public static void main(String[] args) throws IOException  {
		if (args.length != 2) {
			System.out.println("Error: Missing argument properties file and/or num threads");
			System.out.println("Usage: MessageConsumer <consumer properties file> <number_of_threads>");
		}

		InputStream in = new FileInputStream(new File(args[0]));
		Properties properties = new Properties();
		int numberOfThreads = Integer.parseInt(args[1]);
		if (in != null) {
			properties.load(in);
			in.close();
			
			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			for(int i=0; i < numberOfThreads; i++)
				executorService.execute(new AllMessagesConsumer(properties));
	
			executorService.shutdown();
		}
	}
}