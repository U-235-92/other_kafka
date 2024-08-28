package com.other.app.entity;

import org.apache.kafka.common.Uuid;

public class Message {

	private String id;
	private String payload;
	private MessageType messageType;

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}

	public Message() {
		super();
		this.id = Uuid.randomUuid().toString();
	}

	public Message(String payload, MessageType messageType) {
		this();
		this.payload = payload;
		this.messageType = messageType;
	}
	
	public Message(String id, String payload, MessageType messageType) {
		super();
		this.id = id;
		this.payload = payload;
		this.messageType = messageType;
	}

	public String getId() {
		return id;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	@Override
	public String toString() {
		return "Message [id=" + id + ", payload=" + payload + ", messageType=" + messageType + "]";
	}
}
