package com.other.app.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.other.app.entity.Message;
import com.other.app.entity.MessageType;
import com.other.app.util.AppConstants;
import com.other.app.util.MessageDeserializer;
import com.other.app.util.MessageTypeDeseializer;

public class MessageConsumer implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(MessageConsumer.class.getName());
	
	private Properties properties;
	private Consumer<MessageType, Message> consumer;
	
	public MessageConsumer() {
		properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "message-consumers");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MessageTypeDeseializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
		consumer = new KafkaConsumer<MessageType, Message>(properties);
		LOGGER.setLevel(Level.INFO);
	}
	
	@Override
	public void run() {
		consumer.subscribe(Collections.singleton(AppConstants.TOPIC));
		while(true) {
			ConsumerRecords<MessageType, Message> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<MessageType, Message> consumerRecord : records) {
				int partition = consumerRecord.partition();
				String topic = consumerRecord.topic();
				long offset = consumerRecord.offset();
				Message message = consumerRecord.value();
				LOGGER.info("Received a message " + message + "Topic: " + topic + ", Partition: " + partition + ", Offset: " + offset);
			}
		}
	}

	public void shutdown() {
		consumer.wakeup();
		
	}
}
