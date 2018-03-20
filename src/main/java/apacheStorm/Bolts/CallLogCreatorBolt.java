package apacheStorm.Bolts;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//Create a class CallLogCreatorBolt which implement IRichBolt interface
public class CallLogCreatorBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//Create instance for OutputCollector which collects and emits tuples to produce output
	private OutputCollector collector;


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


	public void execute(Tuple tuple) {
		String from = tuple.getString(0);
		// String to = tuple.getString(1);
		//Integer duration = tuple.getInteger(2);
		System.out.println(from+"*************************************");
		collector.emit(new Values(from));
	}


	public void cleanup() {}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call"));
	}


	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}