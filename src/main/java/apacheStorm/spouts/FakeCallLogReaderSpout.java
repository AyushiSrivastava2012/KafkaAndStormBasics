package apacheStorm.spouts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FakeCallLogReaderSpout implements IRichSpout {
	//Create instance for SpoutOutputCollector which passes tuples to bolt.
	private SpoutOutputCollector collector;
	private boolean completed = false;

	//Create instance for TopologyContext which contains topology data.
	private TopologyContext context;

	//Create instance for Random class.
	private Random randomGenerator = new Random();
	private Integer idx = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if(this.idx <= 1000) {
			//			List<String> mobileNumbers = new ArrayList<String>();
			//			mobileNumbers.add("1234123401");
			//			mobileNumbers.add("1234123402");
			//			mobileNumbers.add("1234123403");
			//			mobileNumbers.add("1234123404");
			//
			//			Integer localIdx = 0;
			//			while(localIdx++ < 100 && this.idx++ < 1000) {
			//				String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
			//				String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
			//
			//				while(fromMobileNumber == toMobileNumber) {
			//					toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
			//				}
			//
			//				Integer duration = randomGenerator.nextInt(60);
			//				this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
			//			}

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
				for (ConsumerRecord<Integer, String> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s\n", 
							record.offset(), record.key(), record.value());
					// print the offset,key and value for the consumer records.
					this.collector.emit(new Values(record.value()));
				}

				

			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("from"));
	}

	//Override all the interface methods
	@Override
	public void close() {}

	public boolean isDistributed() {
		return false;
	}

	@Override
	public void activate() {}

	@Override 
	public void deactivate() {}

	@Override
	public void ack(Object msgId) {}

	@Override
	public void fail(Object msgId) {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}