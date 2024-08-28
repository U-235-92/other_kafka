package com.other.app.util;

import java.nio.ByteBuffer;

import org.apache.kafka.common.serialization.Deserializer;

import com.other.app.entity.Message;

public class MessageDeserializer implements Deserializer<Message> {

	
	@Override
	public Message deserialize(String topic, byte[] data) {
		ByteBuffer buffer = ByteBuffer.allocate(data.length);
		buffer.put(data);
		String id = getId(buffer);
		String payload = getPayload(buffer);
		Message message = new Message(id, payload);
		return message;
	}

	private String getId(ByteBuffer buffer) {
		int idLength = buffer.getInt();
		byte[] idBytes = new byte[idLength];
		buffer.get(idBytes);
		String id = new String(idBytes);
		return id;
	}
	
	private String getPayload(ByteBuffer buffer) {
		int payloadLength = buffer.getInt();
		byte[] payloadBytes = new byte[payloadLength];
		buffer.get(payloadBytes);
		String payload = new String(payloadBytes);
		return payload;
	}
}
