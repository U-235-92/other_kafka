package com.other.app.producer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.other.app.entity.Message;
import com.other.app.entity.MessageType;
import com.other.app.util.AppConstants;
import com.other.app.util.MessagePrtitioner;
import com.other.app.util.MessageSerializer;

public class MessageProducer implements Runnable {

	private Producer<String, Message> producer;
	private Properties properties;
	
	public MessageProducer(String producerName) {
		properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerName);
		properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MessagePrtitioner.class.getName());
		producer = new KafkaProducer<String, Message>(properties);
	}
	
	@Override
	public void run() {
		for(int i = 0; i < 10000; i++) {
			sendMessage(new Message("Abra-Kadabra"), MessageType.COMMON);
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void sendMessage(Message message, MessageType messageType) {
		ProducerRecord<String, Message> producerRecord = new ProducerRecord<String, Message>(AppConstants.TOPIC, messageType.name(), message);
		producer.send(producerRecord);
	}
}
