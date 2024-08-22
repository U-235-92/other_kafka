package com.other.app;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MessageConsumer {

	private static final Logger LOGGER = Logger.getLogger(MessageConsumer.class.getName());
	
	private Properties properties;
	private Consumer<Long, String> consumer;
	
	public MessageConsumer() {
		initProperties();
		initConsumer(properties);
		initLogger();
	}

	private void initProperties() {
		properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("group.id", "simple_message_consumer_group");
		properties.put("client.id", "simple_message_consumer");
		properties.put("key.deserializer", LongDeserializer.class.getName());
		properties.put("value.deserializer", StringDeserializer.class.getName());
//		properties.put("enable.auto.commit", "true");
	}

	private void initConsumer(Properties properties) {
		consumer = new KafkaConsumer<Long, String>(properties);
	}

	private void initLogger() {
		LOGGER.setLevel(Level.INFO);
	}
	
	public void consumeMessage() {
		consumer.subscribe(List.of("messages"));
		while(true) {
			ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<Long, String> consumerRecord : consumerRecords) {
				String message = consumerRecord.value();
				long offset = consumerRecord.offset();
				int partition = consumerRecord.partition();
				String topic = consumerRecord.topic();
				System.out.println(Thread.currentThread().getName());
				System.out.println("Recieved message: " + message + " Offset: " + offset + " Partition: " + partition + " Topic: " + topic);
			}
			consumer.commitSync();
		}
	}
}
