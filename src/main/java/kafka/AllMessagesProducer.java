package kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AllMessagesProducer implements Runnable {

	private static String message = "Hello Kafka!!!";
	private Producer<String, String> producer;
	private static Properties properties;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public AllMessagesProducer(Properties props) {
		properties = props;
		producer = new KafkaProducer<String, String>(props);
	}

	public void run() {
		try {
			while (!closed.get()) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
						properties.getProperty("producer_topic"),
						properties.getProperty("partition_name"), message);
				producer.send(producerRecord);
				System.out.println("all-messages-producer done publishing a record!!!");
				Thread.sleep(3000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

	// Shutdown hook
	public void shutdown() {
		closed.set(true);
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.out.println("Error: Missing argument properties file and/or num threads");
			System.out.println("Usage: AllMessagesProducer <producer properties file> <number of threads>");
		}

		InputStream in = new FileInputStream(new File(args[0]));
		Properties properties = new Properties();
		int numberOfThreads = Integer.parseInt(args[1]);
		if (in != null) {
			properties.load(in);
			in.close();
			
			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			for(int i=0; i < numberOfThreads; i++)
				executorService.execute(new AllMessagesProducer(properties));
			
			executorService.shutdown();
		}
	}
}
