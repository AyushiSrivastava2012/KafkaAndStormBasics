package apacheStorm.Bolts;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class CallLogCounterBolt implements IRichBolt {
	Map<String, Integer> counterMap;
	private OutputCollector collector;


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
	}


	public void execute(Tuple tuple) {
		String call = tuple.getString(0);
		System.out.println(call+"**************************************************");
		collector.ack(tuple);
	}


	public void cleanup() {
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call"));
	}


	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}