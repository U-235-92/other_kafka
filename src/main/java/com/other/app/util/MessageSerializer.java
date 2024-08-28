package com.other.app.util;

import java.nio.ByteBuffer;

import org.apache.kafka.common.serialization.Serializer;

import com.other.app.entity.Message;

public class MessageSerializer implements Serializer<Message> {

	@Override
	public byte[] serialize(String topic, Message message) {
		int idLength = message.getId().getBytes().length;
		int payloadLength = message.getPayload().getBytes().length;
		ByteBuffer buffer = ByteBuffer.allocate(4 + idLength + 4 + payloadLength);
		buffer.putInt(idLength);
		buffer.put(message.getId().getBytes());
		buffer.putInt(payloadLength);
		buffer.put(message.getPayload().getBytes());
		return buffer.array();
	}
}
