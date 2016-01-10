package kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

//if partitioner is not set, this will toggle to produce messages to all partitions
public class SinglePartitionMessageProducer implements Runnable {

	private static String message = "Hello Kafka partition!!!";
	private final AtomicBoolean closed = new AtomicBoolean(false);

	private static Properties properties;

	public SinglePartitionMessageProducer(Properties props) {
		properties = props;
		
	}

	public void run() {
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		try {
			while (!closed.get()) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
						properties.getProperty("producer_topic"),
						properties.getProperty("partition_name"), message);
				producer.send(producerRecord);
				System.out.println("single-partition-producer done publishing a record!!!");
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

		if (args.length != 1) {
			System.out.println("Error: Missing argument properties file");
			System.out.println("Usage: SinglePartitionMessageProducer <producer properties file>");
		}

		InputStream in = new FileInputStream(new File(args[0]));
		Properties properties = new Properties();
		if (in != null) {
			properties.load(in);
			in.close();
			SinglePartitionMessageProducer messageProducer = new SinglePartitionMessageProducer(properties);
			new Thread(messageProducer).start();
		}
	}

}
