package com.other.app.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.other.app.entity.Message;
import com.other.app.util.AppConstants;
import com.other.app.util.MessageDeserializer;

public class MessageConsumer implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(MessageConsumer.class.getName());
	
	private Properties properties;
	private Consumer<String, Message> consumer;
	
	public MessageConsumer() {
		properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "message-consumers");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
		consumer = new KafkaConsumer<String, Message>(properties);
		LOGGER.setLevel(Level.INFO);
	}
	
	@Override
	public void run() {
		readMessagesAutoCommit();
	}
	
	@SuppressWarnings("unused")
	private void readFromOffset(long offset) {
		int partition = 1;
		TopicPartition topicPartition = new TopicPartition(AppConstants.TOPIC, partition);
		consumer.assign(Collections.singleton(topicPartition));
		consumer.seek(topicPartition, offset);
		while(true) {
			ConsumerRecords<String, Message> consumerRecords = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, Message> consumerRecord : consumerRecords) {
				Message message = consumerRecord.value();
				offset = consumerRecord.offset();
				LOGGER.info("Received a message " + message + "Topic: " + topicPartition + ", Partition: " + topicPartition + ", Offset: " + offset);
			}
		}
	}
	
	@SuppressWarnings("unused")
	private void readWithFixedOffset() {
		consumer.subscribe(Collections.singleton(AppConstants.TOPIC));
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		int count = 0;
		while(true) {
			ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, Message> consumerRecord : records) {
				int partition = consumerRecord.partition();
				String topic = consumerRecord.topic();
				long offset = consumerRecord.offset();
				Message message = consumerRecord.value();
				LOGGER.info("Received a message " + message + "Topic: " + topic + ", Partition: " + partition + ", Offset: " + offset);
				TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
				OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset());
				offsets.put(topicPartition, offsetAndMetadata);
				if(count % 100 == 0) {
					consumer.commitAsync(offsets, null);
					LOGGER.info("[ASYNC COMMIT CALL] Offset: " + consumerRecord.offset());
				}
				count++;
			}
		}
	}
	
	@SuppressWarnings("unused")
	private void readMessagesAutoCommit() {
		consumer.subscribe(Collections.singleton(AppConstants.TOPIC));
		while(true) {
			ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, Message> consumerRecord : records) {
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
