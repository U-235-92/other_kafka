package com.other.app.util;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import com.other.app.entity.MessageType;

public class MessagePrtitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		int numberOfPartitions = cluster.partitionCountForTopic(topic);
		if(((MessageType) key).compareTo(MessageType.PROMPT) == 0) {
			return numberOfPartitions - 1;
		}
		return Math.abs(Utils.murmur2(keyBytes)) % (numberOfPartitions - 1);
	}

	@Override
	public void close() {}

}
