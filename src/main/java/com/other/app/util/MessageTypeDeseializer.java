package com.other.app.util;

import org.apache.kafka.common.serialization.Deserializer;

import com.other.app.entity.MessageType;

public class MessageTypeDeseializer implements Deserializer<MessageType> {

	@Override
	public MessageType deserialize(String topic, byte[] data) {
		String messageTypeStr = new String(data);
		MessageType messageType = MessageType.valueOf(messageTypeStr);
		return messageType;
	}
}
