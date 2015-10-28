package kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerWakeupException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class SinglePartitionMessageConsumer implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private final TopicPartition topicPartition;

	// if partitioner is not set, this will toggle to consume messages from all
	// partitions
	public SinglePartitionMessageConsumer(Properties properties)
			throws ClassNotFoundException, NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		consumer = new KafkaConsumer<String, String>(properties);

		// trying to dynamically instantiate partitioner from property files
		if (properties.getProperty("partitioner.class") != null) {
			Class<?> partitionerClass = Class.forName(properties.getProperty("partitioner.class"));
			Method partitionMethod = partitionerClass.getMethod("getPartitionIdForKey", byte[].class);

			topicPartition = new TopicPartition(
					properties.getProperty("consumer_topic"),
					(int) partitionMethod.invoke(null,
							properties.getProperty("partition_name").toString().getBytes()));
			consumer.assign(Arrays.asList(topicPartition));
		} else {
			topicPartition = null;
			consumer.subscribe(Arrays.asList(properties.getProperty("consumer_topic")));
		}
	}

	public void run() {
		try {
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
				System.out.println("single-partition-consumer batch done...");
				consumer.commitSync();
			}
		} catch (ConsumerWakeupException e) {
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}

	}

	// Shutdown hook
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		if (args.length != 1) {
			System.out.println("Error: Missing argument properties file");
			System.out.println("Usage: MessageConsumer <consumer properties file>");
		}

		InputStream in = new FileInputStream(new File(args[0]));
		Properties properties = new Properties();
		if (in != null) {
			properties.load(in);
			SinglePartitionMessageConsumer consumer = new SinglePartitionMessageConsumer(properties);
			in.close();
			new Thread(consumer).start();
		}
	}
}