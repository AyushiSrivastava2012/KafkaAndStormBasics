package kafka.consumers;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;;

public class Consumer1 {
	public static void main(String[] args) throws Exception {
//		if(args.length == 0){
//			System.out.println("Enter topic name");
//			return;
//		}
		//Kafka consumer configuration settings
		String topicName = "Hello-Kafka";//args[0].toString();
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", 
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", 
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer
				<Integer, String>(props);

		//Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(topicName));

		//print the topic name
		System.out.println("Subscribed to topic " + topicName);
		int i = 0;

		while (true) {
			ConsumerRecords<Integer, String> records = consumer.poll(100);
			for (ConsumerRecord<Integer, String> record : records)

				// print the offset,key and value for the consumer records.
				System.out.printf("offset = %d, key = %s, value = %s\n", 
						record.offset(), record.key(), record.value());
		}
	}
}
