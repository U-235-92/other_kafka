package com.other.app.entity;

import java.io.Serializable;

import org.apache.kafka.common.Uuid;

public class Message implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String id;
	private String payload;

	public Message() {
		super();
		this.id = Uuid.randomUuid().toString();
	}

	public Message(String payload) {
		this();
		this.payload = payload;
	}
	
	public Message(String id, String payload) {
		super();
		this.id = id;
		this.payload = payload;
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
		return "Message [id=" + id + ", payload=" + payload + "]";
	}
}
