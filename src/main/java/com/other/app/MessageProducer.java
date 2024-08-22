package com.other.app;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageProducer {

	private static final Logger LOGGER = Logger.getLogger(MessageProducer.class.getName());

	private Properties properties;
	private Producer<Long, String> producer;

	public MessageProducer() {
		initProperties();
		initProducer(properties);
		setUpLogger();
	}

	private void initProperties() {
		properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("key.serializer", LongSerializer.class.getName());
		properties.put("value.serializer", StringSerializer.class.getName());
	}

	private void initProducer(Properties properties) {
		producer = new KafkaProducer<Long, String>(properties);
	}

	private void setUpLogger() {
		LOGGER.setLevel(Level.WARNING);
	}

	public void sendMessage(String message) throws InterruptedException, ExecutionException {
		ProducerRecord<Long, String> producerRecord = new ProducerRecord<Long, String>("messages", message);
		RecordMetadata recordMetadata = producer.send(producerRecord).get();
		LOGGER.info("Message sent. Offset: " + recordMetadata.offset() + " Partition: " + recordMetadata.partition() + 
				" Topic: " + recordMetadata.topic());
	}
}
