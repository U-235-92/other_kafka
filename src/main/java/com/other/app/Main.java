package com.other.app;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		MessageConsumer messageConsumer1 = new MessageConsumer();
		MessageConsumer messageConsumer2 = new MessageConsumer();
		MessageConsumer messageConsumer3 = new MessageConsumer();
		MessageProducer messageProducer = new MessageProducer();
		ExecutorService executorService = Executors.newCachedThreadPool();
		Runnable consumerTask1 = () -> {			
			messageConsumer1.consumeMessage();
		};
		Runnable consumerTask2 = () -> {
			messageConsumer2.consumeMessage();
		};
		Runnable consumerTask3 = () -> {
			messageConsumer3.consumeMessage();
		};
		Runnable producerTask = () -> {
			for(int i = 0; i < 10000; i++) {			
				try {
					messageProducer.sendMessage("Hello, Kafka world! " + i);
					TimeUnit.MILLISECONDS.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}			
		};
		executorService.execute(producerTask);
		executorService.execute(consumerTask1);
		executorService.execute(consumerTask2);
		executorService.execute(consumerTask3);
	}
}
