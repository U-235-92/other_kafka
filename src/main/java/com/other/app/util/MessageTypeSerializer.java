package com.other.app.util;

import java.nio.ByteBuffer;

import org.apache.kafka.common.serialization.Serializer;

import com.other.app.entity.MessageType;

public class MessageTypeSerializer implements Serializer<MessageType> {

	@Override
	public byte[] serialize(String topic, MessageType data) {
		int bufferSize = data.name().getBytes().length;
		ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
		buffer.put(data.name().getBytes());
		return buffer.array();
	}

}
