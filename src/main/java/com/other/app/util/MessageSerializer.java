package com.other.app.util;

import java.nio.ByteBuffer;

import org.apache.kafka.common.serialization.Serializer;

import com.other.app.entity.Message;

public class MessageSerializer implements Serializer<Message> {

	private MessageTypeSerializer messageTypeSerializer;
	
	public MessageSerializer(MessageTypeSerializer messageTypeSerializer) {
		this.messageTypeSerializer = messageTypeSerializer;
	}
	
	@Override
	public byte[] serialize(String topic, Message message) {
		byte[] messageTypeBytes = messageTypeSerializer.serialize(topic, message.getMessageType());
		int idLength = message.getId().getBytes().length;
		int payloadLength = message.getPayload().getBytes().length;
		int messageTypeLenght = messageTypeBytes.length;
		ByteBuffer buffer = ByteBuffer.allocate(4 + idLength + 4 + payloadLength + 4 + messageTypeLenght);
		buffer.putInt(idLength);
		buffer.put(message.getId().getBytes());
		buffer.putInt(payloadLength);
		buffer.put(message.getPayload().getBytes());
		buffer.putInt(messageTypeLenght);
		buffer.put(messageTypeBytes);
		return buffer.array();
	}
}
