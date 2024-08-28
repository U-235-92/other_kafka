package com.other.app;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.other.app.consumer.MessageConsumer;
import com.other.app.producer.MessageProducer;

public class Main {

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		MessageProducer messageProducer = new MessageProducer("Messager");
		MessageConsumer messageConsumer = new MessageConsumer();
		executorService.submit(messageProducer);
		executorService.submit(messageConsumer);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			messageConsumer.shutdown();
		}));
	}
}
